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
from docs.trading import RULES
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

    # Calculate global market Moving Avg (Coinmarketcap data)
    n_periods = RULES[freq_str]['MOVING_AVG']['PERIODS']
    mkt = db.market_idx_5t.find({"date":{"$gt":now()-delta(hours=24)}}, {"_id":0})
    df_mkt = pd.DataFrame(list(mkt))
    df_mkt.index = df_mkt['date']
    del df_mkt['date']
    df_mkt = df_mkt.pct_change().rolling(window=int(n_periods/2)).mean() * 100
    mkt_ma = df_mkt.iloc[-1]['mktcap_usd']

    # Calculate Z-Scores, store in dataframe/mongodb
    ops=[]
    for pair in pairs:
        candle = candles.newest(pair, freq_str, df=dfc)
        scores = signals.generate(dfc.loc[pair,freq], candle, mkt_ma=mkt_ma)
        name = 'ZSCORE_' + freq_str.upper()
        dfc[name].loc[pair,freq][-1] = scores['CLOSE']['ZSCORE'].round(3)
        ops.append(UpdateOne(
            {"open_time":candle["OPEN_TIME"], "pair":candle["PAIR"], "freq":candle["FREQ"]},
            {'$set': {name: scores['CLOSE']['ZSCORE']}}
        ))
    db.candles.bulk_write(ops)

    siglog('')
    siglog('-'*80)
    siglog("Cycle #{}: Start".format(n_cycles))
    siglog("Data Refresh: '{}'".format(freq_str))
    siglog("Market is {} ({:+.3f}%)".format("BULLISH" if mkt_ma > 0 else "BEARISH", mkt_ma))
    siglog('-'*80)

    # Evaluate open positions
    holdings = list(db.trades.find({'status':'open', 'pair':{"$in":pairs}}))

    for holding in holdings:
        candle = candles.newest(holding['pair'], freq_str, df=dfc)
        update_holding(holding, candle)

    # Evaluate new positions for other tracked pairs
    inactive = sorted(list(set(pairs) - set([n['pair'] for n in holdings])))

    for pair in inactive:
        candle = candles.newest(pair, freq_str, df=dfc)
        evaluate(candle, mkt_ma=mkt_ma)

    earnings_summary(t1)

    log.info('Trade cycle completed. [%sms]', t1)
    n_cycles +=1

#------------------------------------------------------------------------------
def evaluate(candle, mkt_ma=None):
    """Evaluate opening new trade positions
    """
    df = dfc.loc[candle['PAIR'],freq]
    rules = RULES[freq_str]

    scores = signals.generate(df, candle, mkt_ma=mkt_ma)
    pzscore = scores['CLOSE']['ZSCORE']

    log.info("'{}' {}: {:+.2f} Z-Score ({:+.2f}Δ, {:.8g}P, {:.8g}μ)".format(
        freq_str, candle['PAIR'], scores['CLOSE']['ZSCORE'],
        scores['CLOSE']['ZSCORE'] - scores['OPEN']['ZSCORE'],
        candle['CLOSE'], scores['CLOSE']['MEAN']))

    # Adjust lower resistance proportional to bearishness
    # i.e. bearish score of -0.1%, change z-score resist from -2.0 to -3.0
    if mkt_ma < 0:
        adjuster = 1.25
    else:
        adjuster = 1

    if freq_str == '1m':
        # A) Breakout (high Z-Score)
        breakout = rules['Z-SCORE']['BREAKOUT']

        if pzscore > breakout:
            msg="{:+.2f} Z-Score > {:.2f} Breakout Threshold.".format(pzscore, breakout)
            return open_holding(candle, scores, extra=msg)

        # B) Bounce (bullish, bottom z-score)
        bot_resist = rules['Z-SCORE']['BOT_RESIST'] * adjuster

        if mkt_ma > -0.1 and pzscore < bot_resist:
            msg = "{:+.2f} Close Z-score < {:.2f} Bottom Resistance.".format(
                pzscore, bot_resist)
            return open_holding(candle, scores, extra=msg)

    elif freq_str == '5m':
        # A) Breakout (high Z-Score)
        breakout = rules['Z-SCORE']['BREAKOUT']

        if pzscore > breakout:
            msg="{:+.2f} Z-Score > {:.2f} Breakout Threshold.".format(pzscore, breakout)
            return open_holding(candle, scores, extra=msg)

        # B) Bounce (bullish, bottom z-score)
        bot_resist = rules['Z-SCORE']['BOT_RESIST'] * adjuster

        if mkt_ma > 0 and pzscore < bot_resist:
            msg = "{:+.2f} Close Z-score < {:.2f} Bottom Resistance.".format(
                pzscore, bot_resist)
            return open_holding(candle, scores, extra=msg)

        # C) Uptrend (bullish, +MA, +Z-score)
        """
        ma_periods = rules['MOVING_AVG']['PERIODS']
        ma_thresh = rules['MOVING_AVG']['CANDLE_THRESH']

        rng = df.loc[slice(candle['OPEN_TIME'] - delta(hours=4), candle['OPEN_TIME'])]
        pma = (rng['CLOSE'].pct_change().rolling(ma_periods).mean() * 100).iloc[-1]

        if mkt_ma > 0 and pma > ma_thresh and pzscore > 0.5:
            msg = '{:+.2f}% MA > {:.2f}% Threshold, {:+.2f} X-Score.'.format(
                pma, ma_thresh, xscore)
            return open_holding(candle, scores, extra=msg)
        """

    """
    elif candle['FREQ'] == '1h':
        # D) Bullish. 1h vs 24h X-Score > X_THRESHOLD, 5m vs 1h X-Score > 0.
        xscore_1h = dfx.loc[(pair,3600,86400)].XSCORE[0]
        xscore_5m = dfx.loc[(pair,300,3600)].XSCORE[0]
        if xscore_1h > X_THRESH and xscore_5m > 0:
            open_holding(pair, xscore_1h, dfx.loc[(pair)], dfz.loc[(pair)], 3600, 86400)
    """

#------------------------------------------------------------------------------
def update_holding(holding, candle):
    """
    # TODO: remove ifs. take max(c_5m['close_time'], c_1h['close_time'] instead.
    # TODO: save candle close_time when opening new position, to compare with above dates.
    """
    c1, c2 = holding['buy']['candle'], candle
    df = dfc.loc[holding['pair'],freq]
    scores = signals.generate(df, candle)

    if c2['OPEN_TIME'] < c1['OPEN_TIME']:
        return False

    if freq_str == '1m':
        # Dump if price below buy
        if c2['CLOSE'] < c1['CLOSE']:
            return close_holding(holding, c2, scores)

    # Sell if price < peak since position opened
    pmax = df.loc[slice(c1['OPEN_TIME'], df.iloc[-2].name)]['CLOSE'].max()
    log.info("pmax for {} is {:.6f}".format(holding['pair'], pmax))

    if not np.isnan(pmax) and candle['CLOSE'] < pmax:
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
    pmean = df.loc[slice(t1, df.iloc[-2].name)]['CLOSE'].mean()

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

    siglog("{:>4}Price: {:.8g} ({:+.2f}%, {:.2f}% > mean)".format(
        '', p2, pct_diff(p1, p2), pct_diff(pmean, p2)))
    siglog("{:>4}MA: {:+.2f}%".format('', pma))
    siglog("{:>4}Z-Score: {:.2f} ({:+.4g}Δ)".format('', scores['CLOSE']['ZSCORE'],
        scores['CLOSE']['ZSCORE'] - scores['OPEN']['ZSCORE']))
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
