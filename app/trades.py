# app.trades
import logging
import app
from app import siglog, freqtostr, pertostr, candles, signals
from app.utils import utc_datetime as now, df_to_list
from app.timer import Timer
from docs.config import Z_THRESH, Z_WEIGHTS
from docs.data import BINANCE
log = logging.getLogger('trades')

#------------------------------------------------------------------------------
def update_all():
    """
    """
    siglog('-'*80)
    siglog('EVALUATING DATA')

    pairs = BINANCE['pairs']
    db = app.get_db()
    df_z = signals.generate_dataset(pairs)
    df_wtz = signals.apply_weights(df_z, Z_WEIGHTS)
    trades = list(db.trades.find({'status':'open', 'pair':{"$in": pairs}}))

    # Evaluate existing trades
    active = [ n['pair'] for n in trades]
    for pair in active:
        evaluate(pair, df_wtz.loc[(pair)], df_z.loc[(pair)])

    # Open new trades if above threshold
    inactive = list(set(pairs) - set(active))
    for pair in inactive:
        score = df_wtz.loc[(pair, 3600)]['WA_SCORE'].mean()
        if score > Z_THRESH:
            open_new(pair, score, df_wtz.loc[(pair)], df_z.loc[(pair)])

    summarize(df_wtz)
    signals.save_db(df_z)

    return (df_z, df_wtz)

#------------------------------------------------------------------------------
def evaluate(pair, df_wtz, df_z):
    """
    @df_wtz: pd.dataframe w/ multi-index: (freq, period)
    @df_z: pd.dataframe w/ multi-index (freq, period, dimen)
    """
    db = app.get_db()
    trade = db.trades.find_one({'status':'open', 'pair':pair})
    stats = trade['analysis']['start']

    new_5m = candles.last(pair, 300)['open_time'] > trade['start_time']
    new_1h =  candles.last(pair, 3600)['open_time'] > trade['start_time']

    score_5m = df_wtz.loc[(300,3600)].WA_SCORE
    score_1h = df_wtz.loc[(3600,86400)].WA_SCORE

    # Evaluate trade status on new 1h candle
    if new_1h and score_1h < Z_THRESH:
        sc_diff = (score_1h - stats['mean_score']) / stats['mean_score']
        pc_diff = (score_1h['close'] - trade['buy_price']) / trade['buy_price']

        siglog("OPEN: {} 1h score fell {:+.2f}% to {:+.2f}. Price {:+.2f}%.".format(pair, sc_diff, score_1h, pc_diff))
        close_out(pair, score_1h, df_wtz, df_z)
    elif new_5m and score_5m < Z_THRESH:
        sc_diff = (score_5m - stats['mean_score']) / stats['mean_score']
        pc_diff = (score_5m['close'] - trade['buy_price']) / trade['buy_price']

        siglog("OPEN: {} 5m score fell {:+.2f}% to {:+.2f}. Price {:+.2f}%.".format(pair, sc_diff, score_5m, pc_diff))
        close_out(pair, score_5m, df_wtz, df_z)
    else:
        sc_diff = (score_5m - stats['mean_score']) / stats['mean_score']
        pc_diff = (score_5m['close'] - trade['buy_price']) / trade['buy_price']

        siglog("HODLING: {} score is {:+.2f}% to {:.2f}. Price {:+.2f}%.".format(pair, sc_diff, score_5m, pc_diff))

#------------------------------------------------------------------------------
def open_new(pair, score, df_wtz, df_z):
    """Create or update existing position for zscore above threshold value.
    @score: weighted average trade signal score
    @df_wtz: pd.dataframe w/ multi-index: (freq, period)
    @df_z: pd.dataframe w/ multi-index (freq, per, stat)
    """
    FREQ = 3600
    PERIOD = 86400
    fee_pct = BINANCE['trade_fee_pct']
    vol = BINANCE['volume']
    db = app.get_db()
    curs = db.trades.find({'pair':pair, 'status': 'open'})
    if curs.count() > 0:
        return siglog("OPEN: ({}), {:+.2f} mean zscore.".format(pair, score))

    # Open new position
    close = df_z.loc[(FREQ, PERIOD, 'CANDLE')].CLOSE
    fee_amt = (fee_pct/100) * vol * close
    buy_amt = (vol * close) - fee_amt
    db.trades.insert_one({
        'pair': pair,
        'status': 'open',
        'exchange': 'Binance',
        'start_time': now(),
        'buy_price': close,
        'buy_vol': vol,
        'buy_amt': buy_amt,
        'total_fee_pct': fee_pct,
        'analysis': {
            'start': {
                'candles': df_to_list(df_z),
                'zscores': df_to_list(df_wtz),
                'mean_score':score
            }
        }
    })

    signals.print((pair, FREQ, PERIOD), score, df_z.loc[(FREQ, PERIOD)])
    siglog("OPENING POSITION: ({}), {:+.2f} mean zscore.".format(pair, score))

#------------------------------------------------------------------------------
def close_out(pair, score, df_wtz, df_z):
    """Close off existing position and calculate earnings.
    @score: weighted average trade signal score
    @df_wtz: pd.dataframe w/ multi-index: (freq, period)
    @df_z: pd.dataframe w/ multi-index (freq, per, stat)
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
    close = df_z.loc[(FREQ, PERIOD, 'CANDLE')].CLOSE
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
                'candles': df_to_list(df_z),
                'zscores': df_to_list(df_wtz),
                'mean_score':score
            }
        }}
    )
    siglog('CLOSING POSITION: ({}), {:+.2f}% price.'.format(pair, price_pct_change))
    signals.print((pair, FREQ, PERIOD), score, df_z.loc[(FREQ, PERIOD)])

#------------------------------------------------------------------------------
def summarize(df_wtz):
    t1 = Timer()

    high_5m = df_wtz.xs(300, level=1).sort_values('WA_SCORE').iloc[-1]
    siglog("Top 5m score is {} at {:+.2f}.".format(high_5m.name[0], high_5m['WA_SCORE']))
    high_1h = df_wtz.xs(3600, level=1).sort_values('WA_SCORE').iloc[-1]
    siglog("Top 1h score is {} at {:+.2f}.".format(high_1h.name[0], high_1h['WA_SCORE']))
    high_1d = df_wtz.xs(86400, level=1).sort_values('WA_SCORE').iloc[-1]
    siglog("Top 1d score is {} at {:+.2f}.".format(high_1d.name[0], high_1d['WA_SCORE']))

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
