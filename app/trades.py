import logging
import app
from app import freqtostr, pertostr, candles, signals
from app.utils import utc_datetime as now, df_to_list
from app.timer import Timer
from docs.config import X_THRESH
from docs.data import BINANCE
def siglog(msg): log.log(100, msg)
def pct_diff(a,b): return (b-a)/a
log = logging.getLogger('trades')

#------------------------------------------------------------------------------
def update():
    """
    """
    siglog('-'*80)
    siglog('EVALUATING DATA')

    pairs = BINANCE['pairs']
    db = app.get_db()
    trades = list(db.trades.find({'status':'open', 'pair':{"$in": pairs}}))

    dfz = signals.zscores(pairs)
    dfx = signals.xscores(dfz)

    # Evaluate existing trades
    active = [ n['pair'] for n in trades]
    for pair in active:
        evaluate(pair, dfx.loc[(pair)], dfz.loc[(pair)])

    # Open new trades if above threshold
    for pair in list(set(pairs) - set(active)):
        xscore = dfx.loc[(pair,3600,86400)].XSCORE
        if xscore > X_THRESH:
            open_new(pair, xscore, dfx.loc[(pair)], dfz.loc[(pair)])

    summarize(dfx)
    signals.save_db(dfz)

    return (dfz, dfx)
#------------------------------------------------------------------------------
def evaluate(pair, dfx, dfz):
    """
    @dfx: pd.dataframe w/ multi-index: (freq, period)
    @dfz: pd.dataframe w/ multi-index (freq, period, dimen)
    """
    db = app.get_db()
    trade = db.trades.find_one({'status':'open', 'pair':pair})
    stats = trade['analysis']['start']

    c_5m = candles.last(pair,300)
    c_1h = candles.last(pair,3600)

    x_5m = dfx.loc[(300,3600)].XSCORE
    x_1h = dfx.loc[(3600,86400)].XSCORE

    p_5m = dfz.loc[(300,3600,'CANDLE')].CLOSE
    p_1h = dfz.loc[(3600,86400,'CANDLE')].CLOSE

    # TODO: append new price to position price history
    # TODO: take all 5m close prices since buy-in, calc mean
    # TODO: if mean < price now, hodl. otherwise sell.

    # TODO: remove ifs. take max(c_5m['close_time'], c_1h['close_time'] instead.
    # TODO: save candle close_time when opening new position, to compare with above dates.

    # Evaluate trade status on new 1h candle
    if c_1h['open_time'] > trade['start_time']:
        if pct_diff(p_1h, trade['buy_price']) < 0:
            return close_out(pair, x_1h, dfx, dfz)
    elif c_5m['open_time'] > trade['start_time'] < 0:
        if pct_diff(p_5m, trade['buy_price']):
            return close_out(pair, x_5m, dfx, dfz)

    xdelta = (x_5m - stats['xscore']) / stats['xscore']
    pdelta = (p_5m - trade['buy_price']) / trade['buy_price']

    siglog("HODLING: {} xscore is {:+.2f}% to {:.2f}. Price {:+.2f}%.".format(
        pair, xdelta, x_5m, pdelta))
#------------------------------------------------------------------------------
def open_new(pair, xscore, dfx, dfz):
    """Create or update existing position for zscore above threshold value.
    @xscore: weighted average trade signal score
    @dfx: pd.dataframe w/ multi-index: (freq, period)
    @dfz: pd.dataframe w/ multi-index (freq, per, stat)
    """
    FREQ = 3600
    PERIOD = 86400
    fee_pct = BINANCE['trade_fee_pct']
    vol = BINANCE['volume']

    # Open new position
    close = dfz.loc[(FREQ, PERIOD, 'CANDLE')].CLOSE
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

    signals.print((pair, FREQ, PERIOD), xscore, dfz.loc[(FREQ, PERIOD)])
    siglog("OPENING POSITION: ({}), {:+.2f} xscore.".format(pair, xscore))
#------------------------------------------------------------------------------
def close_out(pair, xscore, dfx, dfz):
    """Close off existing position and calculate earnings.
    @xscore: weighted average trade signal score
    @dfx: pd.dataframe w/ multi-index: (freq, period)
    @dfz: pd.dataframe w/ multi-index (freq, per, stat)
    """
    # TODO: Move into close_out
    xdelta = pct_diff(x_1h, stats['xscore'])
    # TODO: Move into close_out
    siglog("OPEN: {} 1h xscore fell {:+.2f}% to {:+.2f}. Price {:+.2f}%.".format(
        pair, xdelta, x_1h, pdelta))

    FREQ = 3600
    PERIOD = 86400
    fee_pct = BINANCE['trade_fee_pct']
    vol = BINANCE['volume']
    db = app.get_db()
    curs = db.trades.find({'pair':pair, 'status': 'open'})
    if curs.count() == 0:
        return False

    trade = list(curs)[0]
    close = dfz.loc[(FREQ, PERIOD, 'CANDLE')].CLOSE
    end_time = now()
    fee_amt = (fee_pct/100) * vol * close
    sell_amt = (vol * close) - fee_amt
    price_pct_change = (close - trade['buy_price']) / trade['buy_price']
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
            'analysis.end': {
                'candles': df_to_list(dfz),
                'zscores': df_to_list(dfx),
                'xscore': xscore
            }
        }}
    )
    siglog('CLOSING POSITION: ({}), {:+.2f}% price.'.format(pair, price_pct_change))
    signals.print((pair, FREQ, PERIOD), xscore, dfz.loc[(FREQ, PERIOD)])
#------------------------------------------------------------------------------
def summarize(dfx):
    t1 = Timer()

    dfx = dfx.sort_values('XSCORE')
    df_5m = dfx.xs(300, level=1).iloc[-1]
    df_1h = dfx.xs(3600, level=1).iloc[-1]
    df_1d = dfx.xs(86400, level=1).iloc[-1]

    siglog("Top 5m xscore is {} at {:+.2f}.".format(df_5m.name[0], df_5m['XSCORE']))
    siglog("Top 1h xscore is {} at {:+.2f}.".format(df_1h.name[0], df_1h['XSCORE']))
    siglog("Top 1d xscore is {} at {:+.2f}.".format(df_1d.name[0], df_1d['XSCORE']))

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

    win_ratio = n_gain/len(closed) if len(closed) >0 else 0
    pct_net_gain = pct_gross_gain - (len(closed) * pct_trade_fee)
    n_open = db.trades.find({"status":"open"}).count()

    siglog("SUMMARY: %s closed, %s win ratio, %s%% gross earn, %s%% net earn. %s open." %(
        len(closed), round(win_ratio,2), round(pct_gross_gain,2), round(pct_net_gain,2), n_open))
    log.info('%s win trades, %s loss trades.', n_gain, n_loss)
    log.info('Scores/trades updated. [%ss]', t1)
    siglog('-'*80)
