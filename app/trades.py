import logging
from datetime import timedelta
import pandas as pd
import numpy as np
import app
from app import freqtostr, strtofreq, pertostr, candles, signals
from app.utils import utc_datetime as now, df_to_list, to_relative_str
from app.timer import Timer
from docs.config import Z_FACTORS, X_THRESH, Z_BEAR_BOUNCE_THRESH
from docs.data import BINANCE

def siglog(msg): log.log(100, msg)
def pct_diff(a,b): return ((b-a)/a)*100

log = logging.getLogger('trades')
pairs = BINANCE['pairs']
dfc = pd.DataFrame()

#------------------------------------------------------------------------------
def last_candle(pair, freq_str):
    global dfc
    freq = strtofreq[freq_str]
    series = dfc.loc[pair, freq].iloc[-1]
    open_time = dfc.loc[(pair,freq)].index[-1]
    idx = dict(zip(dfc.index.names, [pair, freq_str, open_time]))
    return {**idx, **series}
#------------------------------------------------------------------------------
def update(freq_str):
    """Trading cycle run on each candle refresh. Evaluate open positions and
    compute data signals for opening new positions.
    """
    global dfc
    t1 = Timer()

    # Merge new candle data
    merge_candles(span=timedelta(minutes=10))

    siglog('*'*80)
    siglog('EVALUATING {:} CYCLE'.format(freq_str.upper()))

    # Evaluate open positions
    holdings = list(app.get_db().trades.find({'status':'open', 'pair':{"$in":pairs}}))

    for holding in holdings:
        candle = last_candle(holding['pair'], freq_str)
        update_position(holding, candle)

    # Evaluate new positions for other tracked pairs
    inactive = list(set(pairs) - set([n['pair'] for n in holdings]))

    for pair in inactive:
        candle = last_candle(pair, freq_str)
        evaluate(candle)

    cycle_summary(t1)
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
            siglog("{} {:+.2f} X-Score > {:.2f} Threshold.".format(pair,xscore,X_THRESH))
            return open_position(candle, scores)

        # B) Bounce. 5m vs 3h Close Price Z-Score < Z_BOUNCE_THRESH
        zscore = scores['CLOSE'].loc['ZSCORE']
        if zscore < Z_BEAR_BOUNCE_THRESH:
            siglog("Opening Bounce Trade on ({},300,10800)."\
                "{:+.2f} Price Z-score.".format(pair, zscore))
            return open_position(candle, scores)

        # C) Upward Price Action. 15m MA > 0.1 and X-Score > 0
        #df = candles.load_db(pair, '5m', start=now()-timedelta(hours=2))
        #ma = (df['close'].rolling(5).mean().pct_change()*100).iloc[-1]
        #if ma > 0.1 and xscore > X_THRESH/2:
        #    siglog('Opening trade on {} on positive price action, ({:+.6f} MA).'.format(pair, ma))
        #    open_position(pair, xscore, dfx.loc[(pair)], dfz.loc[(pair)], 300, 3600)

    """
    elif candle['FREQ'] == '1h':
        # D) Bullish. 1h vs 24h X-Score > X_THRESHOLD, 5m vs 1h X-Score > 0.
        xscore_1h = dfx.loc[(pair,3600,86400)].XSCORE[0]
        xscore_5m = dfx.loc[(pair,300,3600)].XSCORE[0]
        if xscore_1h > X_THRESH and xscore_5m > 0:
            open_position(pair, xscore_1h, dfx.loc[(pair)], dfz.loc[(pair)], 3600, 86400)
    """
#------------------------------------------------------------------------------
def open_position(candle, scores):
    """Create or update existing position for zscore above threshold value.
    @xscore: weighted average trade signal score
    @dfx: pd.dataframe w/ multi-index: (freq, period)
    @dfz: pd.dataframe w/ multi-index (freq, per, stat)
    """
    global dfc
    pct_fee = BINANCE['trade_fee_pct']
    quote = BINANCE['trade_amt']
    p = candle['CLOSE']
    fee = quote * (pct_fee/100)
    vol = quote/p - fee

    app.get_db().trades.insert_one({
        'pair': candle['PAIR'],
        'status': 'open',
        'start_time': now(),
        'exchange': 'Binance',
        'buy': {
            'time': now(),
            'price': p,
            'volume': vol,
            'quote': quote,
            'fee': fee,
            'pct_fee': pct_fee,
            'candle': candle,
            'signals': scores.to_dict()
        }
    })

    siglog("OPENING POSITION: ({}), {:+.2f} xscore.".format(
        candle['PAIR'], scores['CLOSE']['XSCORE']))
#------------------------------------------------------------------------------
def update_position(holding, candle):
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
        log_pnl("Closing Position", holding, candle, scores)
        return close_position(holding, candle, scores)

    pmean = dfc.loc[pair,freq]['CLOSE'].loc[
        slice(buy_candle['OPEN_TIME'], dfc.loc[pair,freq].iloc[-2].name
    )].mean()

    if not np.isnan(pmean) and candle['CLOSE'] < pmean:

        return close_position(holding, candle, scores)

    log_pnl("HODLING", holding, candle, scores)
#------------------------------------------------------------------------------
def close_position(holding, candle, scores):
    """Close off existing position and calculate earnings.
    @xscore: weighted average trade signal score
    @dfx: pd.dataframe w/ multi-index: (freq, period)
    @dfz: pd.dataframe w/ multi-index (freq, per, stat)
    """
    global dfc
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

    app.get_db().trades.update_one(
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
        }}
    )

    log_pnl("Closing Position", holding, candle, scores)
#------------------------------------------------------------------------------
def preload_candles():
    """Preload candles records from mongoDB to global dataframe.
    Performance: ~3,000ms/100k records
    """
    global dfc
    merge_candles(span=timedelta(days=21))
#------------------------------------------------------------------------------
def merge_candles(span=None):
    """Merge newly updated candle data from daemon into global dataframe.
    """
    global dfc
    t1 = Timer()

    idx, data = [], []
    span = span if span else timedelta(days=21)

    curs = app.get_db().candles.find(
        {"pair":{"$in":pairs}, "close_time":{"$gte":now()-span}})

    for candle in curs:
        idx.append((candle['pair'], strtofreq[candle['freq']], candle['open_time']))
        data.append([candle[x.lower()] for x in Z_FACTORS])

    df = pd.DataFrame(data,
        index = pd.Index(idx, names=['PAIR', 'FREQ', 'OPEN_TIME']),
        columns = Z_FACTORS)

    dfc = pd.concat([dfc, df]).drop_duplicates().sort_index()

    log.debug("%s candle records merged. [%sms]", len(df), t1)
#------------------------------------------------------------------------------
def log_pnl(msg, holding, candle, scores):
    """Log diff in xscore and price of trade since position opening.
    """
    global dfc
    pair = holding['pair']
    freq = strtofreq[candle['FREQ']]

    x1 = holding['buy']['signals']['CLOSE']['XSCORE']
    x2 = scores['CLOSE']['XSCORE']
    xdelta = pct_diff(x1, x2)
    pmean = dfc.loc[pair,freq]['CLOSE'].loc[
        slice(holding['buy']['candle']['OPEN_TIME'], dfc.loc[pair,freq].iloc[-2].name
    )].mean()
    duration = to_relative_str(now() - holding['start_time'])[:-4]

    siglog("{}: {} ({}) ".format(msg.upper(), pair, duration))

    siglog("{:>4}Xscore: {:+.2f} (Buy), {:+.2f} (Now).".format('', x1, x2))

    siglog("{:>4}Price: {:.8f} (Buy), {:.8f} (Mean, Interim), {:.8f} (Now).".format(
        '', holding['buy']['candle']['CLOSE'], pmean, candle['CLOSE']))
#------------------------------------------------------------------------------
def cycle_summary(t1):
    """
    """
    global dfc
    freq = strtofreq['5m']
    df = pd.DataFrame()

    # Max/Min X-Scores
    for pair in pairs:
        scores = signals.generate(dfc.loc[pair,freq], last_candle(pair, '5m'))
        scores['PAIR'] = pair
        df = df.append(scores)

    _max = df[df.CLOSE == df['CLOSE'].max()].iloc[0]
    log.info("Top {} xscore is {} at {:+.2f}.".format(
        '5m', _max['PAIR'], _max['CLOSE']))

    _min = df[df.CLOSE == df['CLOSE'].min()].iloc[0]
    log.info("Lowest {} xscore is {} at {:+.2f}.".format(
        '5m', _min['PAIR'], _min['CLOSE']))

    # Trading Summary
    db = app.get_db()
    closed = list(db.trades.find({"status":"closed"}))
    n_loss, n_gain = 0, 0
    pct_gross_gain = 0
    pct_net_gain = 0
    pct_trade_fee = 0.05

    for n in closed:
        if n['pct_pdiff'] > 0:
            n_gain += 1
        else:
            n_loss += 1
        pct_gross_gain += n['pct_pdiff']

    win_ratio = (n_gain/len(closed))*100 if len(closed) >0 else 0
    pct_net_gain = pct_gross_gain - (len(closed) * pct_trade_fee)
    n_open = db.trades.find({"status":"open"}).count()

    siglog("SUMMARY:")
    siglog("{:>4}{:}/{:} win ratio ({:+.2f}%). {:} open.".format(
        '', n_gain, len(closed), win_ratio, n_open))
    siglog("{:>4}{:+.2f}% gross earn, {:+.2f}% net earn.".format(
        '', pct_gross_gain, pct_net_gain))
    siglog('*'*80)

    log.info('Trade cycle completed. [%sms]', t1)
