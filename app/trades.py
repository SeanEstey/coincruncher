import logging
from datetime import timedelta as delta
import pandas as pd
import numpy as np
from pymongo import UpdateOne, ReturnDocument
from pprint import pprint
import app
from app import freqtostr, strtofreq, pertostr, candles, signals
from app.utils import utc_datetime as now, df_to_list, to_relative_str
from app.timer import Timer
from docs.rules import RULES
from docs.data import BINANCE

def siglog(msg): log.log(100, msg)
def pct_diff(a,b): return ((b-a)/a)*100

log = logging.getLogger('trades')
pairs = BINANCE['PAIRS']
dfc = pd.DataFrame()
dfz = pd.DataFrame()
n_cycles = 0
freq = None
freq_str = None

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

    log.info('{:,} records loaded in {:,.1f}s.'.format(len(dfc), t1.elapsed(unit='s')))

#------------------------------------------------------------------------------
def update(_freq_str):
    """Called from main app loop on refreshing candle data. Update active
    holdings and evaluate new positions.
    TODO: calculate bearishness/bulliness from Binance candle data
    instead of CMC due to low CMC refresh rate.
    Calculate 5m global market moving avg
    """
    global dfc, n_cycles, freq_str, freq
    freq_str = _freq_str
    freq = strtofreq[freq_str]
    t1 = Timer()
    db = app.get_db()

    # Merge new candle data
    dfc = candles.merge(dfc, pairs, time_span=delta(minutes=10))

    # *********************************************************************
    # FIXME Replace CMC mkt data w/ Binance
    # FIXME: Test if EMA results are better than MA
    signals.pct_market_change(dfc, freq_str)

    n_periods = RULES[freq_str]['MOVING_AVG']['PERIODS']
    mkt = db.market_idx_5t.find(
        {"date":{"$gt":now()-delta(hours=24)}},{"_id":0}).sort("date",1)
    df_mkt = pd.DataFrame(list(mkt))
    df_mkt.index = df_mkt['date']
    del df_mkt['date']
    #df_mkt = df_mkt.pct_change().rolling(window=int(n_periods/2)).mean() * 100
    df_mkt = df_mkt.pct_change().ewm(span=5).mean() * 100
    mkt_ma = df_mkt.iloc[-1]['mktcap_usd']
    # *********************************************************************

    # Calculate Z-Scores, store in dataframe/mongodb
    ops=[]
    for pair in pairs:
        candle = candles.newest(pair, freq_str, df=dfc)
        scores = signals.z_score(
            dfc.loc[pair,freq], candle, mkt_ma=mkt_ma)
        name = 'ZSCORE_' + freq_str.upper()
        #dfc[name].loc[pair,freq][-1] = scores['CLOSE']['ZSCORE'].round(3)
        #ops.append(UpdateOne({"open_time":candle["OPEN_TIME"],
        #    "pair":candle["PAIR"], "freq":candle["FREQ"]},
        #    {'$set': {name: scores['CLOSE']['ZSCORE']}}
        #))
    #db.candles.bulk_write(ops)

    siglog('')
    siglog('-'*80)
    siglog("Cycle #{}: Start".format(n_cycles))
    siglog("Data Refresh: '{}'".format(freq_str))
    siglog("Market is {} ({:+.3f}%)".format(
        "BULLISH" if mkt_ma > 0 else "BEARISH", mkt_ma))
    siglog('-'*80)

    # Evaluate open positions
    holdings = list(db.trades.find({'status':'open', 'pair':{"$in":pairs}}))

    for holding in holdings:
        candle = candles.newest(holding['pair'], freq_str, df=dfc)
        update_holding(holding, candle, mkt_ma=mkt_ma)

    # Evaluate new positions for other tracked pairs
    inactive = sorted(list(set(pairs) - set([n['pair'] for n in holdings])))

    for pair in inactive:
        candle = candles.newest(pair, freq_str, df=dfc)
        evaluate(candle, mkt_ma=mkt_ma)

    earnings_summary(t1)
    n_cycles +=1

#-------------------------------------------------------------------------------
def evaluate(candle, mkt_ma=None):
    """Evaluate opening new trade positions
    """
    df = dfc.loc[candle['PAIR'],freq]
    rules = RULES[freq_str]
    scores = signals.z_score(df, candle, mkt_ma=mkt_ma)
    z_score = scores['CLOSE']['ZSCORE']

    log.debug("'{}' {}: {:+.2f} Z-Score ({:+.2f}Δ, {:.8g}P, {:.8g}μ)".format(
        freq_str, candle['PAIR'], scores['CLOSE']['ZSCORE'],
        scores['CLOSE']['ZSCORE'] - scores['OPEN']['ZSCORE'],
        candle['CLOSE'], scores['CLOSE']['MEAN']))

    # A) Breakout (Close Z-Score > Threshold)
    breakout = rules['Z-SCORE']['BUY_BREAK_REST']
    if z_score > breakout:
        msg="{:+.2f} Z-Score > {:.2f} Breakout.".format(z_score, breakout)
        return open_holding(candle, scores, extra=msg)

    # B) Bounce (Bullish Mkt, Close Z-Score < Support)
    z_support = signals.adjust_support_level(freq_str, mkt_ma)
    if z_score < z_support:
        return open_holding(candle, scores, extra=\
            "{:+.2f} Close Z-score < {:.2f} Support.".format(z_score, z_support))

    # C) Uptrend (Bullish Mkt, +MA, +Z-Scores for Close/Volume/BuyRatio)
    ma_periods = rules['MOVING_AVG']['PERIODS']
    ma_thresh = rules['MOVING_AVG']['CANDLE_THRESH']
    rng = df.loc[slice(candle['OPEN_TIME'] - delta(hours=2), candle['OPEN_TIME'])]
    ma = (rng['CLOSE'].pct_change().rolling(ma_periods).mean() * 100).iloc[-1]
    if mkt_ma > 0.1 and ma > ma_thresh and z_score > 0.5:
        if scores['VOLUME']['ZSCORE'] > 0.5 and scores['BUY_RATIO']['ZSCORE'] > 0.5:
            msg = '{:+.2f}% MA > {:.2f}% Threshold, {:+.2f} Z-Score.'.format(
                ma, ma_thresh, z_score)
            return open_holding(candle, scores, extra=msg)

#------------------------------------------------------------------------------
def update_holding(holding, candle, mkt_ma=None):
    """Evaluate sell potential of holding.
    """
    c1, c2 = holding['buy']['candle'], candle
    df = dfc.loc[holding['pair'],freq]
    scores = signals.z_score(df, candle)

    if c2['OPEN_TIME'] < c1['OPEN_TIME']:
        return False

    # Dump if price below buy
    if c2['CLOSE'] < c1['CLOSE']:
        if 'Resistance' not in holding['buy']['details']:
            return close_holding(holding, c2, scores)

        margin = signals.adjust_support_margin(freq_str, mkt_ma)

        if (c2['CLOSE'] * margin) < c1['CLOSE']:
            return close_holding(holding, c2, scores)
    # Sell if price < peak since position opened
    else:
        p_max = df.loc[slice(c1['OPEN_TIME'], df.iloc[-2].name)]['CLOSE'].max()
        #log.info("pmax for {} is {:.6f}".format(holding['pair'], p_max))

        if not np.isnan(p_max) and candle['CLOSE'] < p_max:
            return close_holding(holding, c2, scores)

    summary(holding, c2, scores=scores)

#------------------------------------------------------------------------------
def open_holding(candle, scores, extra=None):
    """Create or update existing position for zscore above threshold value.
    @scores: z-scores
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
    siglog("{:>4}Price: {:.8g}, BuyRatio: {:.2f}".format('', candle['CLOSE'], candle['BUY_RATIO']))

#------------------------------------------------------------------------------
def close_holding(holding, candle, scores):
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
    rules = RULES[freq_str]
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
    ma_periods = rules['MOVING_AVG']['PERIODS']
    pma = (df_rng['CLOSE'].pct_change().rolling(ma_periods).mean() * 100).iloc[-1]

    siglog("{:>4}Price: {:.8g} ({:+.2f}%)".format(
        '', p2, pct_diff(p1, p2)))
    siglog("{:>4}MA: {:+.2f}%".format('', pma))
    siglog("{:>4}Z-Score: {:.2f} ({:+.4g}Δ)".format('', scores['CLOSE']['ZSCORE'],
        scores['CLOSE']['ZSCORE'] - holding['buy']['signals']['CLOSE']['ZSCORE']))
    siglog("{:>4}Duration: {}".format('', duration))

#------------------------------------------------------------------------------
def earnings_summary(t1):
    """
    """
    db = app.get_db()

    siglog('-'*80)
    siglog("Cycle #{}: Completed in {:,.0f} ms".format(n_cycles, t1.elapsed()))

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
