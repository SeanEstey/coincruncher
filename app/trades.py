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

    siglog('')
    siglog('-'*80)
    siglog("Cycle #{}: Start".format(n_cycles))
    siglog("Data Refresh: '{}'".format(freq_str))
    siglog('-'*80)

    # Evaluate open positions
    holdings = list(app.get_db().trades.find({'status':'open', 'pair':{"$in":pairs}}))

    for holding in holdings:
        candle = candles.newest(holding['pair'], freq_str, df=dfc)
        update_holding(holding, candle)

    # Evaluate new positions for other tracked pairs
    inactive = list(set(pairs) - set([n['pair'] for n in holdings]))

    for pair in inactive:
        candle = candles.newest(pair, freq_str, df=dfc)
        evaluate(candle)

    earnings_summary(t1)

    log.info('Trade cycle completed. [%sms]', t1)
    n_cycles +=1

#------------------------------------------------------------------------------
def evaluate(candle):
    """Evaluate opening new trade positions
    """
    global dfc
    pair = candle['PAIR']
    freq = strtofreq[candle['FREQ']]
    scores = signals.generate(dfc.loc[pair,freq], candle)
    xscore = scores.iloc[-1].CLOSE

    if candle['FREQ'] == '5m':
        # A) Breakout. 5m vs 3h X-Score > X_TRESHOLD
        if xscore > X_THRESH:
            msg="{:+.2f} X-Score > {:.2f} Threshold.".format(xscore, X_THRESH)
            return open_holding(candle, scores, extra=msg)

        # B) Bounce. 5m vs 3h Close Price Z-Score < Z_BOUNCE_THRESH
        zscore = scores['CLOSE'].loc['ZSCORE']
        if zscore < Z_BOUNCE_THRESH:
            msg = "{:+.2f} Close Z-score < {:.2f} Bounce Threshold.".format(
                zscore, Z_BOUNCE_THRESH)
            return open_holding(candle, scores, extra=msg)

        # C) Upward Price action on MA and X-Score > THRESH/2
        from docs.config import MA_WINDOW, MA_THRESH
        start = candle['OPEN_TIME'] - timedelta(hours=4)
        df_rng = dfc.loc[pair,freq].loc[slice(start, candle['OPEN_TIME'])]
        ma = df_rng['CLOSE'].rolling(MA_WINDOW).mean().pct_change() * 100
        pma = ma.iloc[-1]

        if pma > MA_THRESH and xscore > X_THRESH/2:
            msg = '{:+.6f}% MA% > {:.2f}% Threshold (Window={}).'.format(
                pma, MA_THRESH, MA_WINDOW)
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
def update_holding(holding, candle):
    """
    # TODO: remove ifs. take max(c_5m['close_time'], c_1h['close_time'] instead.
    # TODO: save candle close_time when opening new position, to compare with above dates.
    """
    global dfc
    pair = holding['pair']
    freq = strtofreq[candle['FREQ']]
    scores = signals.generate(dfc.loc[pair, freq], candle)
    buy_candle = holding['buy']['candle']

    if candle['OPEN_TIME'] < buy_candle['OPEN_TIME']:
        return False

    if candle['CLOSE'] < buy_candle['CLOSE']:
        return close_holding(holding, candle, scores)

    pmean = dfc.loc[pair,freq]['CLOSE'].loc[
        slice(buy_candle['OPEN_TIME'], dfc.loc[pair,freq].iloc[-2].name
    )].mean()

    if not np.isnan(pmean) and candle['CLOSE'] < pmean:
        return close_holding(holding, candle, scores)

    summary(holding)

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

    app.get_db().trades.insert_one({
        'pair': candle['PAIR'],
        'status': 'open',
        'start_time': now(),
        'exchange': 'Binance',
        'buy': {
            'time': now(),
            'price': candle['CLOSE'],
            'volume': quote / candle['CLOSE'],
            'quote': quote + fee,
            'fee': fee,
            'pct_fee': pct_fee,
            'candle': candle,
            'signals': scores.to_dict(),
            'extra': extra if extra else None
        }
    })
    siglog("BOUGHT {}".format(candle['PAIR']))
    siglog("{:>4}X-Score: {:+.2f}".format('', scores['CLOSE']['XSCORE']))
    siglog("{:>4}Details: {}".format('', extra))

#------------------------------------------------------------------------------
def close_holding(holding, candle, scores):
    """Close off existing position and calculate earnings.
    @xscore: weighted average trade signal score
    @dfx: pd.dataframe w/ multi-index: (freq, period)
    @dfz: pd.dataframe w/ multi-index (freq, per, stat)
    """
    pct_fee = BINANCE['trade_fee_pct']
    buy_candle = holding['buy']['candle']
    buy_vol = holding['buy']['volume']
    buy_quote = holding['buy']['quote']
    p1, p2 = buy_candle['CLOSE'], candle['CLOSE']
    pct_pdiff = pct_diff(p1, p2)
    quote = (p2 * buy_vol) * (1 - pct_fee/100)
    fee = (p2 * buy_vol) * (pct_fee/100)

    net_earn = quote - buy_quote
    pct_net = pct_diff(buy_quote, quote)

    db = app.get_db()
    holding = db.trades.find_one_and_update(
        {'_id': holding['_id']},
        {'$set': {
            'status': 'closed',
            'end_time': now(),
            'pct_pdiff': pct_pdiff,
            'pct_earn': pct_net,
            'net_earn': net_earn,
            'sell': {
                'time': now(),
                'price': p2,
                'volume': buy_vol,
                'quote': quote,
                'fee': fee,
                'pct_fee': pct_fee,
                'candle': candle,
                'signals': scores.to_dict()
            }
        }},
        return_document=ReturnDocument.AFTER
    )
    summary(holding)

#------------------------------------------------------------------------------
def summary(holding):
    """Log diff in xscore and price of trade since position opening.
    """
    global dfc
    pair = holding['pair']
    # TODO: FIXME
    #freq = strtofreq[candle['FREQ']]
    freq = 300
    df = dfc.loc[pair,freq]
    candle = candles.newest(pair, freqtostr[freq], df=dfc)

    x1 = holding['buy']['signals']['CLOSE']['XSCORE']
    p1 = holding['buy']['candle']['CLOSE']
    t1 = holding['buy']['candle']['OPEN_TIME']
    pmean = df.loc[slice(t1, df.iloc[-2].name)]['CLOSE'].mean()

    if holding['status'] == 'open':
        siglog("Hodling {}".format(pair))
        p2 = candle['CLOSE']
        x2 = signals.generate(df, candle)['CLOSE']['XSCORE']
    elif holding['status'] == 'closed':
        profit = holding['net_earn'] > 0
        siglog("Sold {} ({:+.2f} {})".format(pair,
            holding['pct_earn'], 'PROFIT' if profit else 'LOSS'))
        p2 = holding['sell']['candle']['CLOSE']
        x2 = holding['sell']['signals']['CLOSE']['XSCORE']

    siglog("{:>4}X-Score: {:.2f} ({:+.0f}%)".format(
        '', x2, pct_diff(x1, x2)))

    siglog("{:>4}Price: {:.8g} ({:+.2f}%, {:.2f}% > mean)".format(
        '', p2, pct_diff(p1, p2), pct_diff(pmean, p2)))

    duration = to_relative_str(now() - holding['start_time'])[:-4]
    siglog("{:>4}Duration: {}".format('', duration))

#------------------------------------------------------------------------------
def earnings_summary(t1):
    """
    """
    global dfc, n_cycles
    db = app.get_db()
    # TODO: FIXME
    freq_str = '5m'
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

    siglog("Holdings: {} Open, {:+.2f}% Mean Value".format(len(active), sum(pct_change_hold)/len(pct_change_hold)))
    siglog('-'*80)

    # Log Max/Min X-Scores
    df = pd.DataFrame()
    for pair in pairs:
        scores = signals.generate(dfc.loc[pair,freq], candles.newest(pair, '5m', df=dfc))
        scores['PAIR'] = pair
        df = df.append(scores)

    df = df.xs('XSCORE')
    _max = df[df.CLOSE == df['CLOSE'].max()].iloc[0]
    log.info("Top {} xscore is {} at {:+.2f}.".format(
        '5m', _max['PAIR'], _max['CLOSE']))

    _min = df[df.CLOSE == df['CLOSE'].min()].iloc[0]
    log.info("Lowest {} xscore is {} at {:+.2f}.".format(
        '5m', _min['PAIR'], _min['CLOSE']))

    lines = df.to_string().split("\n")
    db.signals.replace_one({"_id":{"$exists":True}}, {"scores":lines}, upsert=True)
