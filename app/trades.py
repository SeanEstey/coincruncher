import logging
from datetime import timedelta as delta
import pandas as pd
import numpy as np
from pymongo import UpdateOne, ReturnDocument
from pprint import pprint
import app
from app import freqtostr, strtofreq, pertostr, candles, signals
from app.utils import utc_datetime as now, to_relative_str
from app.timer import Timer
from docs.rules import RULES
from docs.data import BINANCE

def siglog(msg): log.log(100, msg)
def pct_diff(a,b): return ((b-a)/a)*100
log = logging.getLogger('trades')

# ****** FIXME *******
rules = RULES['1m']
# ********************

dfc = pd.DataFrame()
n_cycles = 0
mkt_move, freq, freq_str = None, None, None
pairs = BINANCE['PAIRS']
ma_periods = rules['MOVING_AVG']['PERIODS']
ma_thresh = rules['MOVING_AVG']['CANDLE_THRESH']
z_thresh = rules['Z-SCORE']['THRESH']

#------------------------------------------------------------------------------
def init():
    """Preload candles records from mongoDB to global dataframe.
    Performance: ~3,000ms/100k records
    """
    global dfc
    t1 = Timer()
    log.info('Preloading historic data...')

    dfc = pd.DataFrame()
    dfc = candles.merge(dfc, pairs, time_span=delta(days=21))

    log.info('{:,} records loaded in {:,.1f}s.'.format(
        len(dfc), t1.elapsed(unit='s')))

#------------------------------------------------------------------------------
def update(_freq_str):
    """Evaluate Binance market data and execute buy/sell trades.
    """
    global dfc, n_cycles, freq_str, freq, mkt_move
    freq_str = _freq_str
    freq = strtofreq[freq_str]
    t1 = Timer()
    db = app.get_db()

    # Merge new candle data
    dfc = candles.merge(dfc, pairs, time_span=delta(minutes=10))

    # Bullish/Bearish market movement
    mkt_move = signals.pct_market_change(dfc, freq_str)
    mkt_5m = signals.pct_market_change(dfc, '5m')

    siglog('-'*80)
    siglog("Cycle #{}: Start".format(n_cycles))
    siglog("Data Refresh: '{}'".format(freq_str))
    lines = mkt_5m.to_string(
        formatters={mkt_5m.columns[0]: '{:+,.4f}%'.format}).split("\n")
    [siglog(x) for x in lines]
    lines = mkt_move.to_string(
        formatters={mkt_move.columns[0]: '{:+,.4f}%'.format}).split("\n")
    [siglog(x) for x in lines]
    siglog('-'*80)

    # Evaluate open positions
    holdings = list(db.trades.find({'status':'open', 'pair':{"$in":pairs}}))

    for holding in holdings:
        candle = candles.newest(holding['pair'], freq_str, df=dfc)
        eval_sell(holding, candle)

    # Evaluate new positions for other tracked pairs
    inactive = sorted(list(set(pairs) - set([n['pair'] for n in holdings])))

    for pair in inactive:
        candle = candles.newest(pair, freq_str, df=dfc)
        eval_buy(candle)

    earnings(t1)
    n_cycles +=1

#-------------------------------------------------------------------------------
def eval_buy(candle):
    """New trade criteria.
    """
    cc,cot,cp = candle['CLOSE'], candle['OPEN_TIME'], candle['PAIR']
    df = dfc.loc[candle['PAIR'],freq]
    scores = signals.z_score(df, candle)
    zb,zp,zo,zv = scores['BUY_RATIO']['ZSCORE'], scores['CLOSE']['ZSCORE'],\
        scores['OPEN']['ZSCORE'], scores['VOLUME']['ZSCORE']
    log.debug("{} {:+.2f} ZP, {:+.2f} ΔZP, {:.8g} P".format(cp.lower(), zp, zp-zo, cc))

    # A. Z-Score below threshold
    if zp < z_thresh:
        return buy(candle, scores, 'zthresh', extra="{:+.2f} ZP < {:.2f}.".format(
            zp, z_thresh))

    # B) Positive market & candle EMA (within Z-Score threshold)
    rng = df.loc[slice(cot - delta(hours=2), cot)]
    mslope = mkt_move.loc['Close'][0]
    pema = rng['CLOSE'].ewm(span=10).mean().pct_change()

    if (pema.tail(5) > 0).all():
        if mslope > 0 and zv > 0.5 and zb > 0.5:
            details = '{:+.8g} mslope, {:+.8g}% pslope, {:+.2f} zp.'.format(mslope, pema.iloc[-1], zp)
            return buy(candle, scores, 'pslope', extra=details)

#------------------------------------------------------------------------------
def eval_sell(holding, candle):
    """Avoid losses, maximize profits.
    """
    cot = candle['OPEN_TIME']
    df = dfc.loc[holding['pair'],freq]
    scores = signals.z_score(df, candle)
    zp = scores['CLOSE']['ZSCORE']
    reason = holding['buy'].get('reason','')

    # A. Predict price peak as we approach mean value.
    if reason == 'zthresh':
        if zp > -0.75:
            return sell(holding, candle, scores, 'zthresh')
    # B. Sell when price slope < 0
    elif reason == 'pslope':
        rng = df.loc[slice(cot - delta(hours=2), cot)]
        pslope = rng['CLOSE'].ewm(span=10).mean().pct_change().iloc[-1]

        if pslope <= 0:
            return sell(holding, candle, scores, 'pslope')

    summary(holding, candle, scores=scores)

#------------------------------------------------------------------------------
def buy(candle, scores, reason, extra=None):
    """Create or update existing position for zscore above threshold value.
    """
    pct_fee = BINANCE['PCT_FEE']
    quote = BINANCE['TRADE_AMT']
    fee = quote * (pct_fee/100)

    app.get_db().trades.insert_one({
        'pair': candle['PAIR'],
        'status': 'open',
        'start_time': now(),
        'exchange': 'Binance',
        'buy': {
            'time': now(),
            'reason': reason,
            'price': candle['CLOSE'].round(8),
            'volume': np.float64(quote / candle['CLOSE']).round(8),
            'quote': quote + fee,
            'fee': fee,
            'pct_fee': pct_fee,
            'candle': candle,
            'signals': scores.to_dict(),
            'details': extra if extra else None
        }
    })
    siglog("BOUGHT {}".format(candle['PAIR']))
    siglog("{:>4}Details: {}".format('', extra))
    siglog("{:>4}Price: {:.8g}, BuyRatio: {:.2f}".format('', candle['CLOSE'],
        candle['BUY_RATIO']))

#------------------------------------------------------------------------------
def sell(holding, candle, scores, reason):
    """Close off existing position and calculate earnings.
    """
    pct_fee = BINANCE['PCT_FEE']
    buy_vol = np.float64(holding['buy']['volume'])
    buy_quote = np.float64(holding['buy']['quote'])
    p1 = np.float64(holding['buy']['candle']['CLOSE'])

    p2 =candle['CLOSE']
    pct_pdiff = pct_diff(p1, p2)
    quote = (p2 * buy_vol) * (1 - pct_fee/100)
    fee = (p2 * buy_vol) * (pct_fee/100)

    net_earn = quote - buy_quote
    pct_net = pct_diff(buy_quote, quote)

    duration = now() - holding['start_time']
    candle['BUY_RATIO'] = candle['BUY_RATIO'].round(4)

    db = app.get_db()
    holding = db.trades.find_one_and_update(
        {'_id': holding['_id']},
        {'$set': {
            'status': 'closed',
            'end_time': now(),
            'duration': int(duration.total_seconds()),
            'pct_pdiff': pct_pdiff.round(4),
            'pct_earn': pct_net.round(4),
            'net_earn': net_earn.round(4),
            'sell': {
                'time': now(),
                'reason': reason,
                'price': p2.round(8),
                'volume': buy_vol.round(8),
                'quote': quote.round(8),
                'fee': fee,
                'pct_fee': pct_fee,
                'candle': candle,
                'signals': scores.to_dict()
            }
        }},
        return_document=ReturnDocument.AFTER
    )
    summary(holding, candle, scores=scores)

#------------------------------------------------------------------------------
def summary(holding, candle, scores=None):
    """
    """
    pair = holding['pair']
    df = dfc.loc[holding['pair'],freq]
    p1 = holding['buy']['candle']['CLOSE']
    t1 = holding['buy']['candle']['OPEN_TIME']

    if holding['status'] == 'open':
        siglog("Hodling {}".format(pair))
        p2 = candle['CLOSE']
    elif holding['status'] == 'closed':
        profit = holding['net_earn'] > 0
        siglog("Sold {} ({:+.2f}% {})".format(pair,
            holding['pct_earn'], 'PROFIT' if profit else 'LOSS'))
        p2 = holding['sell']['candle']['CLOSE']

    duration = to_relative_str(now() - holding['start_time'])

    df_rng = df.loc[slice(candle['OPEN_TIME'] - delta(hours=4), candle['OPEN_TIME'])]
    pma = (df_rng['CLOSE'].pct_change().rolling(ma_periods).mean() * 100).iloc[-1]

    siglog("{:>4}Price: {:.8g} ({:+.2f}%)".format(
        '', p2, pct_diff(p1, p2)))
    siglog("{:>4}MA: {:+.2f}%".format('', pma))
    siglog("{:>4}Z-Score: {:.2f} ({:+.4g}Δ)".format('', scores['CLOSE']['ZSCORE'],
        scores['CLOSE']['ZSCORE'] - holding['buy']['signals']['CLOSE']['ZSCORE']))
    siglog("{:>4}Duration: {}".format('', duration))

#------------------------------------------------------------------------------
def earnings(t1):
    """
    """
    db = app.get_db()

    siglog('-'*80)
    siglog("Completed in {:,.0f} ms".format(t1.elapsed()))

    # Closed trades
    n_win, pct_earn = 0, 0
    closed = list(db.trades.find({"status":"closed"}))

    for n in closed:
        if n['pct_pdiff'] > 0:
            n_win += 1
        pct_earn += n['pct_earn']

    ratio = (n_win/len(closed))*100 if len(closed) >0 else 0

    siglog("History: {}/{} Wins ({:.0f}%), {:+.2f}% Net Earn".format(
        n_win, len(closed), ratio, pct_earn))

    # Open Trades (If Sold at Present Value)
    pct_change_hold = []
    active = list(db.trades.find({'status':'open'}))
    for holding in active:
        candle = candles.newest(holding['pair'], freq_str, df=dfc)
        pct_change_hold.append(pct_diff(holding['buy']['candle']['CLOSE'], candle['CLOSE']))

    if len(pct_change_hold) > 0:
        pct_change_hold = sum(pct_change_hold)/len(pct_change_hold)
    else:
        pct_change_hold = 0.0

    siglog("Holdings: {} Open, {:+.2f}% Mean Value".format(len(active), pct_change_hold))
    siglog('-'*80)

#------------------------------------------------------------------------------
def _unfinished():
    # *********************************************************************
    # Calculate Z-Scores, store in dataframe/mongodb
    # ops=[]
    # for pair in pairs:
    #    candle = candles.newest(pair, freq_str, df=dfc)
    #    scores = signals.z_score(
    #        dfc.loc[pair,freq], candle, mkt_ma=mkt_ma)
    #    name = 'ZSCORE_' + freq_str.upper()
    #   dfc[name].loc[pair,freq][-1] = scores['CLOSE']['ZSCORE'].round(3)
    #   ops.append(UpdateOne({"open_time":candle["OPEN_TIME"],
    #       "pair":candle["PAIR"], "freq":candle["FREQ"]},
    #       {'$set': {name: scores['CLOSE']['ZSCORE']}}
    #   ))
    #   db.candles.bulk_write(ops)
    #
    #   if c2['OPEN_TIME'] < c1['OPEN_TIME']:
    #       return False
    # *********************************************************************

    # ********************************************************************
    # A. Profit loss
    # if c2['CLOSE'] < c1['CLOSE']:
    #    if 'Resistance' not in holding['buy']['details']:
    #        return sell(holding, c2, scores)
    #    margin = signals.adjust_support_margin(freq_str, mkt_ma)
    #    if (c2['CLOSE'] * margin) < c1['CLOSE']:
    #        return sell(holding, c2, scores)
    # B. Maximize profit, make sure price still rising.
    # p_max = df.loc[slice(c1['OPEN_TIME'], df.iloc[-2].name)]['CLOSE'].max()
    # elif not np.isnan(p_max) and candle['CLOSE'] < p_max:
    #   return sell(holding, c2, scores)
    # ********************************************************************
    pass

