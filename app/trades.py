# app.trades
import logging
import app
from app import candles, signals
from app.utils import utc_datetime as now
from app.timer import Timer
from docs.data import FREQ_STR as freqtostr, PER_STR as pertostr
from docs.config import Z_THRESH, Z_WEIGHTS
from docs.data import BINANCE

log = logging.getLogger('trades')
def siglog(msg): log.log(100, msg)
def keystostr(keys): return (keys[0], freqtostr[keys[1]], pertostr[keys[2]])

#------------------------------------------------------------------------------
def df_to_list(df):
    return df.to_string().title().split("\n")

#------------------------------------------------------------------------------
def update_all():
    """Compute pair/aggregate signal data. Binance candle historical averages..
    """
    pairs = BINANCE['pairs']
    t1 = Timer()
    # Aggregate zscores from candles
    df_z = signals.generate_dataset(pairs)
    signals.save_db(df_z)
    df_wtz = signals.weight_scores(df_z, Z_WEIGHTS)

    # Mean across 3 historical periods on 1h candles
    for pair in df_wtz.index.levels[0]:
        score = df_wtz.loc[(pair, 3600)]['weighted'].mean()
        if score > Z_THRESH:
            update(pair, score, df_wtz.loc[(pair)], df_z.loc[(pair)])
        else:
            close(pair, score, df_wtz.loc[(pair)], df_z.loc[(pair)])

    high_1h = df_wtz.xs(3600, level=1).sort_values('weighted').iloc[-1]
    siglog("Highest 1h score is {} at {:+.2f}.".format(high_1h.name, high_1h['weighted']))
    high_5m = df_wtz.xs(300, level=1).sort_values('weighted').iloc[-1]
    siglog("Highest 5m score is {} at {:+.2f}.".format(high_5m.name, high_5m['weighted']))

    summarize()
    log.info('Scores/trades updated. [%ss]', t1)
    return (df_z, df_wtz)

#------------------------------------------------------------------------------
def update(pair, score, df_wtz, df_z):
    """Create or update existing position for zscore above threshold value.
    @score: weighted average trade signal score
    @df_wtz: dataframe w/ index
    @df_z: dataframe w/ index (freq, per, stat)
    """
    fee_pct = BINANCE['trade_fee_pct']
    vol = BINANCE['volume']

    db = app.get_db()
    curs = db.trades.find({'pair':pair, 'status': 'open'})
    if curs.count() > 0:
        return siglog("OPEN: ({}), {:+.2f} mean zscore.".format(
            ",".join(keystostr((pair, 3600, 86400))), score))

    # Open new position
    close = df_z.loc[(3600, 86400, 'candle')].close
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

    signals.print_score(
        (pair, 3600, 86400), score, df_z.loc[(3600, 86400)])
    siglog("OPENING POSITION: ({}), {:+.2f} mean zscore.".format(
        ",".join(keystostr((pair, 3600, 86400))), score))

#------------------------------------------------------------------------------
def close(pair, score, df_wtz, df_z):
    """Close off existing position and calculate earnings.
    @score: weighted average trade signal score
    @df_z: index (freq, per, stat)
    """
    fee_pct = BINANCE['trade_fee_pct']
    vol = BINANCE['volume']

    db = app.get_db()
    curs = db.trades.find({'pair':pair, 'status': 'open'})
    if curs.count() == 0:
        return False

    trade = list(curs)[0]
    close = df_z.loc[(3600, 86400, 'candle')].close
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
    siglog('CLOSING POSITION: ({}), {:+.2f}% price.'.format(
        ",".join(keystostr((pair, 3600, 86400))), price_pct_change))
    signals.print_score(
        (pair, 3600, 86400), score, df_z.loc[(3600, 86400)])

#------------------------------------------------------------------------------
def summarize():
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
    siglog('-'*80)
    siglog("SUMMARY: %s closed, %s win ratio, %s%% gross earn, %s%% net earn. %s open." %(
        len(closed), round(win_ratio,2), round(pct_gross_gain,2), round(pct_net_gain,2), n_open))
    siglog('-'*80)
    log.info('%s win trades, %s loss trades.', n_gain, n_loss)
