# app.trades
import logging
import app

log = logging.getLogger('trades')

# Temp Values
VOLUME = 1
TRADE_FEE = 0.05

#------------------------------------------------------------------------------
def update_position(keys, wzscore):
    """Create or update existing position for zscore above threshold value.
    """
    db = app.get_db()
    idx = dict(zip(['pair', 'freq', 'period'], keys))
    curs = db.trades.find({**idx: **{"end_time":False}})

    # Open new position
    if curs.count() == 0:
        db.trades.insert_one(**idx, **{
            'exchange': 'Binance',
            'buy_time': now(),
            'buy_price': "ADDME",
            'sell_time': False,
            'sell_price': False,
            'price_pct_change':False,
            'gross_earn':False,
            'net_earn':False,
            'candles': {
                'start': candles.last(idx['pair'], idx['freq']),
            },
            'scores': {'start': {
                'wa_score':zscore, 'data': signals.load_scores(keys).to_dict()
            }}
        })
        siglog("Opened trade for (%s,%s,%s)." % keys)
    # Update open position
    else:
        record = list(curs)[0]
        db.trades.update_one({"_id":record["_id"]}, {"$set":{"last_zscore":zscore}})
        siglog("Updated trade zscore for (%s,%s,%s)." % keys)

#------------------------------------------------------------------------------
def close_position(keys, zscore):
    """Close off existing position and calculate earnings.
    """
    db = app.get_db()
    idx = dict(zip(['pair', 'freq', 'period'], keys))
    curs = db.trades.find({**idx: **{"end_time":False}})

    if curs.count() == 0:
        return False

    trade = list(curs)[0]
    end_time = now()
    candle = candles.last(idx['pair'], idx['freq'])
    scores = signals.load_scores(keys).to_dict()
    buy_price = trade['candles']['start']['close']
    price_pct = candle['close'] - buy_price / buy_price

    trade_fees = "ADDME" + trade['total_fee']
    gross_earn = (candle['close'] * VOLUME) - (trade['candles']['open']['open'] * VOLUME)
    net_earn = gross_earn - trade_fees

    db.trades.update_one(
        {"_id": trade['_id']},
        {"$set": {
            'end_time': end_time,
            'duration': end_time - trade['start_time'],
            'trade_fees': "ADDME",
            'gross_earn': "ADDME",
            'net_earn': "ADDME",
            'last_zscore': zscore,
            'candles.close': candles.last(idx['pair'], idx['freq']),
            'scores.close.mean': zscore,
            'scores.close.comp': signals.load_scores(keys).to_dict(),
            'price_pct': "ADDME"
        }}
    )

    #'end_price': float(df_h.loc[key]['close']['candle']),
    #'pct_change': pct_change,

    siglog('Closing (%s, %s, %s) trade. %s%% price change.' %(
        key[0],key[1],key[2], round(pct_change,2)))

#------------------------------------------------------------------------------
def summarize():
    db = app.get_db()

    closed = list(db.trade_stats.find({"end_time":{"$ne":False}}))
    n_loss, n_gain = 0, 0
    pct_gross_gain = 0
    pct_net_gain = 0
    pct_trade_fee = 0.05

    for n in closed:
        if n['pct_change'] > 0:
            n_gain += 1
        else:
            n_loss += 1
        pct_gross_gain += n['pct_change']

    win_ratio = n_gain/len(closed)
    pct_net_gain = pct_gross_gain - (len(closed) * pct_trade_fee)
    n_open = db.trade_stats.find({"end_time":False}).count()

    log.log(100, "Trade summary: %s closed, %s win ratio, %s%% gross earn, %s%% net earn. %s open." %(
        len(closed), round(win_ratio,2), round(pct_gross_gain,2), round(pct_net_gain,2), n_open))
    log.log(100, '-'*80)
    log.debug('%s win trades, %s loss trades.', n_gain, n_loss)
