import logging
from datetime import timedelta
import pandas as pd
import numpy as np
from pymongo import ReturnDocument
from pprint import pprint
import app
from app import freqtostr, strtofreq, pertostr, candles, signals
from app.utils import utc_datetime as now, df_to_list, to_relative_str
from app.timer import Timer
from docs.config import Z_FACTORS, X_THRESH, Z_BOUNCE_THRESH
from docs.data import BINANCE

def siglog(msg): log.log(100, msg)
def pct_diff(a,b): return ((b-a)/a)*100

log = logging.getLogger('trades')
pairs = BINANCE['pairs']
dfc = pd.DataFrame()
dfz = pd.DataFrame()
n_cycles = 0

#------------------------------------------------------------------------------
def init():
    """Preload candles records from mongoDB to global dataframe.
    Performance: ~3,000ms/100k records
    """
    global dfc
    t1 = Timer()
    log.info('Preloading historic data...')

    dfc = pd.DataFrame()
    dfc = candles.merge(dfc, pairs, time_span=timedelta(days=21))

    log.info('{:,} records loaded in {:,.1f}s.'.format(len(dfc), t1.elapsed(unit='s')))

#------------------------------------------------------------------------------
def update(freq_str):
    """Called from main app loop on refreshing candle data. Update active
    holdings and evaluate new positions.
    """
    global dfc, n_cycles
    t1 = Timer()
    db = app.get_db()

    # Merge new candle data
    dfc = candles.merge(dfc, pairs, time_span=timedelta(minutes=10))

    # Get '5m' global market MA
    from docs.config import MA_WINDOW, MA_THRESH
    mkt = db.market_idx_5t.find({"date":{"$gt":now()-timedelta(hours=24)}}, {"_id":0})
    df_mkt = pd.DataFrame(list(mkt))
    df_mkt.index = df_mkt['date']
    del df_mkt['date']
    df_mkt = df_mkt.pct_change().rolling(window=int(MA_WINDOW/2)).mean() * 100
    mkt_ma = df_mkt.iloc[-1]['mktcap_usd']

    siglog('')
    siglog('-'*80)
    siglog("Cycle #{}: Start".format(n_cycles))
    siglog("Data Refresh: '{}'".format(freq_str))
    siglog("Market is {} ({:+.3f}%)".format("BULLISH" if mkt_ma > 0 else "BEARISH", mkt_ma))
    siglog('-'*80)

    # Evaluate open positions
    holdings = list(app.get_db().trades.find({'status':'open', 'pair':{"$in":pairs}}))

    for holding in holdings:
        candle = candles.newest(holding['pair'], freq_str, df=dfc)
        update_holding(holding, candle, mkt_ma=mkt_ma)

    # Evaluate new positions for other tracked pairs
    inactive = list(set(pairs) - set([n['pair'] for n in holdings]))

    for pair in inactive:
        candle = candles.newest(pair, freq_str, df=dfc)
        evaluate(candle, mkt_ma=mkt_ma)

    earnings_summary(freq_str, t1)

    log.info('Trade cycle completed. [%sms]', t1)
    n_cycles +=1

#------------------------------------------------------------------------------
def evaluate(candle, mkt_ma=None):
    """Evaluate opening new trade positions
    """
    from docs.config import MA_WINDOW, MA_THRESH
    global dfc
    pair = candle['PAIR']
    freq = strtofreq[candle['FREQ']]
    start = candle['OPEN_TIME'] - timedelta(hours=4)
    df_rng = dfc.loc[pair,freq].loc[slice(start, candle['OPEN_TIME'])]

    scores = signals.generate(dfc.loc[pair,freq], candle)
    xscore = signals.xscore(scores.xs('ZSCORE'))
    pzscore = scores['CLOSE'].loc['ZSCORE']
    pma = (df_rng['CLOSE'].pct_change().rolling(MA_WINDOW).mean() * 100).iloc[-1]

    log.info('{} X-Score: {:+.2f}'.format(pair, xscore))

    if candle['FREQ'] == '5m':
        # A) Breakout. X-Score > threshold
        if xscore > X_THRESH:
            msg="{:+.2f} X-Score > {:.2f} Threshold.".format(xscore, X_THRESH)
            return open_holding(candle, scores, extra=msg)

        # B) Bounce. Bullish market AND price Z-Score < threshold
        if mkt_ma > 0 and pzscore < Z_BOUNCE_THRESH:
            msg = "{:+.2f} Close Z-score < {:.2f} Bounce Threshold.".format(
                pzscore, Z_BOUNCE_THRESH)
            return open_holding(candle, scores, extra=msg)

        # C) Uptrend. Bullish market AND Upward price action on MA AND X-Score > THRESH/2
        if mkt_ma > 0 and pma > 0 and xscore > 0.5:
            msg = '{:+.2f}% MA > {:.2f}% Threshold, {:+.2f} X-Score.'.format(
                pma, MA_THRESH, xscore)
            return open_holding(candle, scores, extra=msg)

    """
    elif candle['FREQ'] == '1h':
        # D) Bullish. 1h vs 24h X-Score > X_THRESHOLD, 5m vs 1h X-Score > 0.
        xscore_1h = dfx.loc[(pair,3600,86400)].XSCORE[0]
        xscore_5m = dfx.loc[(pair,300,3600)].XSCORE[0]
        if xscore_1h > X_THRESH and xscore_5m > 0:
            open_holding(pair, xscore_1h, dfx.loc[(pair)], dfz.loc[(pair)], 3600, 86400)
    """

#------------------------------------------------------------------------------
def update_holding(holding, candle, mkt_ma=None):
    """
    # TODO: remove ifs. take max(c_5m['close_time'], c_1h['close_time'] instead.
    # TODO: save candle close_time when opening new position, to compare with above dates.
    """
    global dfc
    pair = holding['pair']
    freq = strtofreq[candle['FREQ']]
    scores = signals.generate(dfc.loc[pair, freq], candle)
    xscore = signals.xscore(scores.xs('ZSCORE'))
    buy_candle = holding['buy']['candle']

    if candle['OPEN_TIME'] < buy_candle['OPEN_TIME']:
        return False

    if candle['FREQ'] == '1m':
        # Dump if bearish market AND low X-Score, OR price < buy_price
        if xscore < 0 or candle['CLOSE'] < buy_candle['CLOSE']:
            return close_holding(holding, candle, scores)

    elif candle['FREQ'] == '5m':
        # Dump if bearish market AND low X-Score, OR price < buy_price
        if (mkt_ma < 0 and xscore < 1.0) or candle['CLOSE'] < buy_candle['CLOSE']:
            return close_holding(holding, candle, scores)

        pmean = dfc.loc[pair,freq].loc[
            slice(buy_candle['OPEN_TIME'], dfc.loc[pair,freq].iloc[-2].name
        )]['CLOSE'].mean()
        pmax = dfc.loc[pair,freq].loc[
            slice(buy_candle['OPEN_TIME'], dfc.loc[pair,freq].iloc[-2].name
        )]['CLOSE'].max()

        # Take profits
        if not np.isnan(pmax) and candle['CLOSE'] < pmax:
            return close_holding(holding, candle, scores)

    summary(holding, candle, scores=scores)

#------------------------------------------------------------------------------
def open_holding(candle, scores, extra=None):
    """Create or update existing position for zscore above threshold value.
    @xscore: weighted average trade signal score
    @dfx: pd.dataframe w/ multi-index: (freq, period)
    @dfz: pd.dataframe w/ multi-index (freq, per, stat)
    """
    pct_fee = BINANCE['trade_fee_pct']
    quote = BINANCE['trade_amt']
    fee = quote * (pct_fee/100)
    xscore = signals.xscore(scores.xs('ZSCORE'))

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
            'signals': {**scores.to_dict(), **{'xscore':xscore}},
            'details': extra if extra else None
        }
    })
    siglog("BOUGHT {}".format(candle['PAIR']))
    siglog("{:>4}Details: {}".format('', extra))
    siglog("{:>4}Price: {:.8g}, BuyRatio: {:.2f}".format('', candle['CLOSE'], candle['BUY_RATIO']))

#------------------------------------------------------------------------------
def close_holding(holding, candle, scores):
    """Close off existing position and calculate earnings.
    @xscore: weighted average trade signal score
    @dfx: pd.dataframe w/ multi-index: (freq, period)
    @dfz: pd.dataframe w/ multi-index (freq, per, stat)
    """
    pct_fee = BINANCE['trade_fee_pct']
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
    """Log diff in xscore and price of trade since position opening.
    """
    from docs.config import MA_WINDOW, MA_THRESH
    global dfc
    pair = holding['pair']
    freq = strtofreq[candle['FREQ']]
    df = dfc.loc[pair,freq]

    x1 = holding['buy']['signals']['xscore']
    x2 = signals.xscore(scores.xs('ZSCORE'))

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

    duration = to_relative_str(now() - holding['start_time'])[:-4]

    df_rng = df.loc[slice(candle['OPEN_TIME'] - timedelta(hours=4), candle['OPEN_TIME'])]
    pma = (df_rng['CLOSE'].pct_change().rolling(MA_WINDOW).mean() * 100).iloc[-1]

    siglog("{:>4}Price: {:.8g} ({:+.2f}%, {:.2f}% > mean)".format(
        '', p2, pct_diff(p1, p2), pct_diff(pmean, p2)))
    siglog("{:>4}MA: {:+.2f}%".format('', pma))
    siglog("{:>4}X-Score: {:.2f} ({:+.0f}%)".format(
        '', x2, pct_diff(x1, x2)))
    siglog("{:>4}Duration: {}".format('', duration))

#------------------------------------------------------------------------------
def earnings_summary(freq_str, t1):
    """
    """
    global dfc, n_cycles
    db = app.get_db()
    freq = strtofreq[freq_str]

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

    # Log Max/Min X-Scores
    df = pd.DataFrame()
    for pair in pairs:
        scores = signals.generate(dfc.loc[pair,freq], candles.newest(pair, '5m', df=dfc))
        scores['PAIR'] = pair
        df = df.append(scores)
    dfx = df.xs('ZSCORE')
    dfx = dfx.reset_index().set_index('PAIR')
    del dfx['index']
    xscores=[]
    for i in range(0, len(dfx)):
        xscores.append(signals.xscore(dfx.iloc[i]))
    dfx = pd.DataFrame(xscores, index=dfx.index, columns=['XSCORE'])
    log.info("Top {} xscore is {} at {:+.2f}.".format('5m', dfx['XSCORE'].idxmax(), dfx['XSCORE'].max()))
    log.info("Lowest {} xscore is {} at {:+.2f}.".format('5m', dfx['XSCORE'].idxmin(), dfx['XSCORE'].min()))
    zlines = df.to_string().split("\n")
    xlines = dfx.sort_values('XSCORE').to_string().split("\n")
    db.signals.replace_one({"_id":{"$exists":True}}, {"zscores":zlines, "xscores":xlines}, upsert=True)
