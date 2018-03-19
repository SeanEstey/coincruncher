import logging
from datetime import timedelta
import pandas as pd
import numpy as np
import app
from app import freqtostr, strtofreq, pertostr, candles, signals
from app.utils import utc_datetime as now, df_to_list
from app.timer import Timer
from docs.config import X_THRESH, Z_BEAR_BOUNCE_THRESH
from docs.data import BINANCE
def siglog(msg): log.log(100, msg)
def pct_diff(a,b): return ((b-a)/a)*100
log = logging.getLogger('trades')
_1m = timedelta(minutes=1)
_1h = timedelta(hours=1)
_1d = timedelta(hours=24)

#------------------------------------------------------------------------------
def update(freqstr):
    """
    """
    siglog('*'*80)
    siglog('EVALUATING {:} CYCLE'.format(freqstr.upper()))
    pairs = BINANCE['pairs']
    db = app.get_db()
    trades = list(db.trades.find({'status':'open', 'pair':{"$in": pairs}}))

    t1 = Timer()
    dfz = pd.DataFrame()
    # Calculate z-scores for various (pair, freq, period) keys
    for pair in pairs:
        c5m = candles.last(pair,"5m")
        t5m = [c5m["open_time"] - (5*_1m), c5m["close_time"] - (5*_1m)]
        c1h = candles.last(pair,"1h")
        t1h = [c1h["open_time"] - (1*_1h), c1h["close_time"] - (1*_1h)]
        c1d = candles.last(pair,"1d")
        t1d = [c1d["open_time"] - (1*_1d), c1d["close_time"] - (1*_1d)]

        for n in range(1,4):
            dfz = dfz.append([
                signals.zscore(pair, "5m", str(n*60)+"m", t5m[0]-(n*60*_1m), t5m[1], c5m),
                signals.zscore(pair, "1h", str(n*24)+"h", t1h[0]-(n*24*_1h), t1h[1], c1h),
                signals.zscore(pair, "1d", str(n*7)+"d",  t1d[0]-(n*7*_1d), t1d[1], c1d)
            ])
    dfz = dfz.sort_index()
    # Save so don't need to calculate each update cycle
    signals.save_db(dfz)

    dfx = signals.xscore(dfz)

    log.debug("[%s rows x %s cols] z-score dataset built. [%ss]",
        len(dfz), len(dfz.columns), t1.elapsed(unit='s'))

    # Evaluate existing trades
    active = [ n['pair'] for n in trades]
    for pair in active:
        evaluate(pair, freqstr, dfx.loc[(pair)], dfz.loc[(pair)])

    inactive = list(set(pairs) - set(active))

    # Evaluate opening new trade positions

    if freqstr == '5m':
        # A) Breakout. 5m vs 3h X-Score > X_TRESHOLD
        for pair in inactive:
            xscore = dfx.loc[(pair,300,10800)].XSCORE[0]
            if xscore > X_THRESH:
                open_position(pair, xscore, dfx.loc[(pair)], dfz.loc[(pair)], 300, 10800)
        # B) Bounce. 5m vs 3h Close Price Z-Score < Z_BOUNCE_THRESH
        for pair in inactive:
            zscore = dfz.loc[(pair,300,10800)].xs('ZSCORE',level=1).CLOSE[0]
            xscore = dfx.loc[(pair,300,3600)].XSCORE[0]
            if zscore < Z_BEAR_BOUNCE_THRESH:
                siglog("Opening Bounce Trade on ({},300,10800). {:+.2f} Price Z-score.".format(pair, zscore))
                open_position(pair, xscore, dfx.loc[(pair)], dfz.loc[(pair)], 300, 10800)
        # C) Upward Price Action. 15m MA > 0.1 and X-Score > 0
        for pair in inactive:
            df = candles.load_db(pair, '5m', start=now()-timedelta(hours=2))
            ma = (df['close'].rolling(3).mean().pct_change()*100).iloc[-1]
            xscore = dfx.loc[(pair,300,3600)].XSCORE[0]
            if ma > 0.1 and xscore > 0:
                xscore = dfx.loc[(pair,300,3600)].XSCORE[0]
                siglog('Opening trade on {} on positive price action, ({:+.6f} MA).'.format(pair, ma))
                open_position(pair, xscore, dfx.loc[(pair)], dfz.loc[(pair)], 300, 3600)
    elif freqstr == '1h':
        # D) Bullish. 1h vs 24h X-Score > X_THRESHOLD, 5m vs 1h X-Score > 0.
        for pair in inactive:
            xscore_1h = dfx.loc[(pair,3600,86400)].XSCORE[0]
            xscore_5m = dfx.loc[(pair,300,3600)].XSCORE[0]
            if xscore_1h > X_THRESH and xscore_5m > 0:
                open_position(pair, xscore_1h, dfx.loc[(pair)], dfz.loc[(pair)], 3600, 86400)

    log_summary(dfx)

    return (dfz, dfx)
#------------------------------------------------------------------------------
def evaluate(pair, freqstr, dfx, dfz):
    """
    @dfx: pd.dataframe w/ multi-index: (freq, period)
    @dfz: pd.dataframe w/ multi-index (freq, period, dimen)
    """
    db = app.get_db()
    trade = db.trades.find_one({'status':'open', 'pair':pair})
    stats = trade['scoring']['start']
    freq = strtofreq[freqstr]
    if freq == 300:
        period = 3600
    elif freq == 3600:
        period = 86400

    c = candles.last(pair,freq)
    x = dfx.loc[(freq,period)].XSCORE[0]
    p = dfz.loc[(freq,period)].CLOSE[0]

    # TODO: remove ifs. take max(c_5m['close_time'], c_1h['close_time'] instead.
    # TODO: save candle close_time when opening new position, to compare with above dates.

    if c['open_time'] > trade['start_time']:
        if c['close'] < trade['buy_price']:
            log_pnl("Closing Position", pair, trade, x, c)
            return close_position(pair, x, dfx, dfz)

        pmean = candles.load_db(pair, freqstr, start=trade['start_time']
            )['close'].mean()
        if not np.isnan(pmean) and p < pmean:
            log_pnl("Closing Position", pair, trade, x, c)
            return close_position(pair, x, dfx, dfz)

    print('c.close: %s, p: %s' %(c['close'], p))
    # Position remains open. Log profits.
    log_pnl("HODLING", pair, trade, x, c)
#------------------------------------------------------------------------------
def open_position(pair, xscore, dfx, dfz, freq, period):
    """Create or update existing position for zscore above threshold value.
    @xscore: weighted average trade signal score
    @dfx: pd.dataframe w/ multi-index: (freq, period)
    @dfz: pd.dataframe w/ multi-index (freq, per, stat)
    """
    #FREQ = 300
    #PERIOD = 86400
    fee_pct = BINANCE['trade_fee_pct']
    vol = BINANCE['volume']

    # Open new position

    # Use 5m price as it's usually more recent than 1h price
    close = dfz.loc[(300,3600)].xs('CANDLE',level=1).CLOSE[0]
    fee_amt = (fee_pct/100) * vol * close
    buy_amt = (vol * close) - fee_amt
    app.get_db().trades.insert_one({
        'pair': pair,
        'status': 'open',
        'exchange': 'Binance',
        'start_time': now(),
        'buy_price': close,
        'buy_vol': vol,
        'buy_amt': buy_amt,
        'total_fee_pct': fee_pct,
        'scoring': {
            'start': {
                'zscores': df_to_list(dfz),
                'xscores': df_to_list(dfx),
                'xscore':xscore
            }
        }
    })

    signals.log_scores((pair, freq, period), xscore, dfz.loc[(freq, period)])
    siglog("OPENING POSITION: ({}), {:+.2f} xscore.".format(pair, xscore))
#------------------------------------------------------------------------------
def close_position(pair, xscore, dfx, dfz):
    """Close off existing position and calculate earnings.
    @xscore: weighted average trade signal score
    @dfx: pd.dataframe w/ multi-index: (freq, period)
    @dfz: pd.dataframe w/ multi-index (freq, per, stat)
    """
    FREQ = 3600
    PERIOD = 86400
    fee_pct = BINANCE['trade_fee_pct']
    vol = BINANCE['volume']
    db = app.get_db()
    curs = db.trades.find({'pair':pair, 'status': 'open'})
    if curs.count() == 0:
        return False

    trade = list(curs)[0]
    close = dfz.loc[(300,3600)].xs('CANDLE',level=1).CLOSE[0]
    end_time = now()
    fee_amt = (fee_pct/100) * vol * close
    sell_amt = (vol * close) - fee_amt
    price_pct_change = pct_diff(trade['buy_price'], close)
    gross_earn = (close * vol) - (trade['buy_price'] * vol)
    net_earn = gross_earn - (fee_amt * 2)

    db.trades.update_one(
        {'_id': trade['_id']},
        {'$set': {
            'status': 'closed',
            'end_time': end_time,
            'sell_price': close,
            'sell_amt': sell_amt,
            'total_fee_pct': fee_pct * 2,
            'price_pct_change': price_pct_change,
            'gross_earn':gross_earn,
            'net_earn': net_earn,
            'scoring.end': {
                'candles': df_to_list(dfz),
                'zscores': df_to_list(dfx),
                'xscore': xscore
            }
        }}
    )

    # TODO: Move into close_out
    #xdelta = pct_diff(xscore, stats['xscore'])
    # TODO: Move into close_out
    #siglog("OPEN: {} 1h xscore fell {:+.2f}% to {:+.2f}. Price {:+.2f}%.".format(
    #    pair, xdelta, xscore, pdelta))

    siglog('CLOSING POSITION: ({}), {:+.2f}% price.'.format(pair, price_pct_change))
    signals.log_scores((pair, FREQ, PERIOD), xscore, dfz.loc[(FREQ, PERIOD)])
#------------------------------------------------------------------------------
def log_pnl(header, pair, trade, xscore, candle):
    """Log diff in xscore and price of trade since position opening.
    """
    from app.utils import to_relative_str

    xscore_buy = trade['scoring']['start']['xscore']
    xdelta = pct_diff(xscore_buy, xscore)
    pinterim = candles.load_db(pair,'5m',start=trade['start_time'])['close'].mean()
    duration = to_relative_str(now() - trade['start_time'])[:-4]

    siglog("{}: {} ({}) ".format(header.upper(), pair, duration))
    siglog("{:>4}Xscore: {:+.2f} (Buy), {:+.2f} (Now).".format('', xscore_buy, xscore))
    siglog("{:>4}Price: {:.8f} (Buy), {:.8f} (Mean, Interim), {:.8f} (Now).".format(
        '', trade['buy_price'], pinterim, candle['close']))
#------------------------------------------------------------------------------
def log_summary(dfx):
    """
    """
    t1 = Timer()

    # Max/Min X-Scores
    dfx = dfx.sort_values('XSCORE')
    for freq in [300, 3600]:
        df_max = dfx.xs(freq, level=1).iloc[-1]
        log.info("Top {} xscore is {} at {:+.2f}.".format(
            freqtostr[freq], df_max.name[0], df_max['XSCORE']))
    for freq in [300, 3600]:
        df_min = dfx.xs(freq, level=1).iloc[0]
        log.info("Lowest {} xscore is {} at {:+.2f}.".format(
            freqtostr[freq], df_min.name[0], df_min['XSCORE']))

    # Trading Summary
    db = app.get_db()
    closed = list(db.trades.find({"status":"closed"}))
    n_loss, n_gain = 0, 0
    pct_gross_gain = 0
    pct_net_gain = 0
    pct_trade_fee = 0.05

    for n in closed:
        if n['price_pct_change'] > 0:
            n_gain += 1
        else:
            n_loss += 1
        pct_gross_gain += n['price_pct_change']

    win_ratio = (n_gain/len(closed))*100 if len(closed) >0 else 0
    pct_net_gain = pct_gross_gain - (len(closed) * pct_trade_fee)
    n_open = db.trades.find({"status":"open"}).count()

    siglog("SUMMARY:")
    siglog("{:>4}{:}/{:} win ratio ({:+.2f}%). {:} open.".format(
        '', n_gain, len(closed), win_ratio, n_open))
    siglog("{:>4}{:+.2f}% gross earn, {:+.2f}% net earn.".format(
        '', pct_gross_gain, pct_net_gain))
    log.info('Scores/trades updated. [%ss]', t1)
    siglog('*'*80)
