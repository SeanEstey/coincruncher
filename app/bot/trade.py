# app.bot.trade
import time
import logging
import pytz
import numpy as np
import pandas as pd
from pymongo import ReplaceOne, UpdateOne
from collections import OrderedDict as odict
from binance.client import Client
from datetime import timedelta as delta, datetime
from docs.conf import *
from docs.botconf import *
import app, app.bot
from app.bot import pct_diff, candles, macd, reports, signals
from app.common.timeutils import strtofreq
from app.common.utils import to_local, utc_datetime as now, to_relative_str
from app.common.timer import Timer

def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)

db = None
log = logging.getLogger('trade')
client = None

#------------------------------------------------------------------------------
def init(client_=None):
    t1 = Timer()
    global client, db
    db = app.get_db()
    client = client_ if client_ else Client('','')

    # Get available exchange trade pairs
    info = client.get_exchange_info()
    ops = [ UpdateOne({'symbol':n['symbol']}, {'$set':n},
        upsert=True) for n in info['symbols'] ]
    db.assets.bulk_write(ops)
    print("{} available trading pairs retrieved from api.".format(len(ops)))

    enabled = db.assets.find({'botTradeStatus':'ENABLED'})
    if enabled.count() == 0:
        raise Exception("No tradepairs enabled")

    pairs = [ n['symbol'] for n in list(enabled) ]

    print("{} enabled pairs: {}".format(enabled.count(), pairs))
    print('{} trading algorithms.'.format(len(TRADE_ALGOS)))

    # Load revelent historic candle data from DB, query any (pair,freq)
    # index data that's missing.
    app.bot.dfc = candles.load(pairs, TRADEFREQS, dfm=pd.DataFrame())
    tuples = pd.MultiIndex.from_product([pairs, TRADEFREQS]).values.tolist()

    for idx in tuples:
        if (idx[0], strtofreq(idx[1])) in app.bot.dfc.index:
            continue
        print("Retrieving {} candle data...".format(idx))
        candles.update([idx[0]], [idx[1]], client=client)
        app.bot.dfc = candles.load([idx[0]], [idx[1]], dfm=app.bot.dfc)

    print("{} historic candles loaded.".format(len(app.bot.dfc)))

    print('Initialized in {:,.0f} ms.'.format(t1.elapsed()))
    print('Ready to trade, sir!')

#---------------------------------------------------------------------------
def run():
    from main import q_closed
    global client
    n_cycles = 0
    client = client if client else Client('','')

    while True:
        exited, entered = None, None
        n=0

        while q_closed.empty() == False:
            candle = q_closed.get()
            ss = snapshot(candle)
            exited = eval_exit(candle, ss)
            entered = eval_entry(candle, ss)
            n+=1

        if n > 0:
            print('{} queue candles cleared.'.format(n))
            if entered:
                reports.trades(entered)
            if db.trades.find({'status':'open'}).count() > 0:
                reports.positions()
            if entered or exited:
                reports.earnings()
        else:
            print('closed candle queue empty.')

        time.sleep(10)

#------------------------------------------------------------------------------
def stoploss():
    """consume from q_open and sell any trades if price falls below stop
    loss threshold.
    """
    from main import q_open

    while True:
        n=0
        while q_open.empty() == False:
            c = q_open.get()
            query = {'pair':c['pair'], 'freq':c['freqstr'], 'status':'open'}

            for trade in db.trades.find(query):
                diff = pct_diff(trade['orders'][0]['candle']['close'], c['close'])

                if diff < trade['stoploss']:
                    sell(trade, c, snapshot(c), details='Stop Loss')
            n+=1

        print('{} stoploss queue items cleared.'.format(n))
        time.sleep(10)

#------------------------------------------------------------------------------
def get_enabled_pairs():
    enabled = db.assets.find({'botTradeStatus':'ENABLED'})
    return [ n['symbol'] for n in list(enabled) ]

#------------------------------------------------------------------------------
def enable_pairs(authpairs):
    """To disable all pairs/freq, pass in empty list.
    """
    result = db.assets.update_many({},
        {'$set':{'botTradeStatus':'DISABLED', 'botTradeFreq':[]}})

    print("Reset {}/{} pairs.".format(
        result.modified_count,
        db.meta.find({'botTradeStatus':'ENABLED'}).count()))

    ops = [
        UpdateOne({'symbol':pair},
            {'$set':{'botTradeStatus':'ENABLED'},
            '$push':{'botTradeFreq':TRADEFREQS}},
            upsert=True) for pair in authpairs
    ]
    if len(ops) > 0:
        result = db.assets.bulk_write(ops)
        print("{} pairs updated, {} upserted.".format(
            result.modified_count, result.upserted_count))

#------------------------------------------------------------------------------
def eval_entry(candle, ss):
    ids = []
    for algo in TRADE_ALGOS:
        if db.trades.find_one({
            'freq':candle['freqstr'],
            'algo':algo['name'],
            'status':'open'
        }): continue

        # Test all filters/conditions eval to True
        if all([fn(candle,ss) for fn in algo['entry']['filters']]):
            if all([fn(candle,ss) for fn in algo['entry']['conditions']]):
                ids.append(buy(candle, algo, ss))
    return ids

#------------------------------------------------------------------------------
def eval_exit(candle, ss):
    ids = []
    query = {
        'pair':candle['pair'],
        'freq':candle['freqstr'],
        'status':'open'
    }
    for trade in db.trades.find(query):
        algo = [n for n in TRADE_ALGOS if n['name'] == trade['algorithm']][0]

        # Test all filters/conditions eval to True
        if all([fn(candle, ss, trade) for fn in algo['exit']['filters']]):
            if all([fn(candle, ss, trade) for fn in algo['exit']['conditions']]):
                ids.append(sell(trade, candle, ss,
                    details="Algo filters/conditions met"))
            else:
                db.trades.update_one({"_id":trade["_id"]},
                    {"$push": {"snapshots":ss}})
    return ids

#------------------------------------------------------------------------------
def buy(candle, algoconf, ss):
    """Create or update existing position for zscore above threshold value.
    """
    global client
    orderbook = client.get_orderbook_ticker(symbol=candle['pair'])
    bid = np.float64(orderbook['bidPrice'])
    ask = np.float64(orderbook['askPrice'])
    meta = db.assets.find_one({'symbol':candle['pair']})

    result = db.trades.insert_one(odict({
        'pair': candle['pair'],
        'quote_asset': meta['quoteAsset'],
        'freq': candle['freqstr'],
        'status': 'open',
        'start_time': now(),
        'algo': algoconf['name'],
        'stoploss': algoconf['stoploss'],
        'snapshots': [ss],
        'orders': [odict({
            'action':'BUY',
            'ex': 'Binance',
            'time': now(),
            'price': ask,
            'pct_spread': pct_diff(bid, ask),
            'pct_slippage': pct_diff(candle['close'], ask),
            'volume': 1.0,
            'quote': TRADE_AMT_MAX,
            'fee': TRADE_AMT_MAX * (BINANCE_PCT_FEE/100),
            'orderbook': orderbook,
            'candle': candle
        })]
    }))

    print("BUY {} ({})".format(candle['pair'], algoconf['name']))
    return result.inserted_id

#------------------------------------------------------------------------------
def sell(record, candle, ss, details=None):
    """Close off existing position and calculate earnings.
    """
    global client
    orderbook = client.get_orderbook_ticker(symbol=candle['pair'])
    bid = np.float64(ss['orderBook']['bidPrice'])
    ask = np.float64(orderbook['askPrice'])
    pct_spread = pct_diff(bid, ask)
    pct_slippage = pct_diff(candle['close'], bid)
    pct_total_slippage = record['orders'][0]['pct_slippage'] + pct_slippage

    pct_fee = BINANCE_PCT_FEE
    buy_vol = np.float64(record['orders'][0]['volume'])
    buy_quote = np.float64(record['orders'][0]['quote'])
    p1 = np.float64(record['orders'][0]['price'])

    pct_gain = pct_diff(p1, bid)
    quote = buy_quote * (1 - pct_fee/100)
    fee = (bid * buy_vol) * (pct_fee/100)
    pct_net_gain = net_earn = pct_gain - (pct_fee*2)

    duration = now() - record['start_time']
    candle['buy_ratio'] = candle['buy_ratio'].round(4)

    print("SELL {} ({}) Details: {}"\
        .format(candle['pair'], record['algo'], details))

    db.trades.update_one(
        {'_id': record['_id']},
        {
            '$push': {
                'snapshots':ss,
                'orders': odict({
                    'action': 'SELL',
                    'ex': 'Binance',
                    'time': now(),
                    'price': bid,
                    'pct_spread': round(pct_spread,3),
                    'pct_slippage': round(pct_slippage,3),
                    'volume': 1.0,
                    'quote': buy_quote,
                    'fee': fee,
                    'orderbook': ss['orderBook'],
                    'candle': candle,
                })
            },
            '$set': {
                'status': 'closed',
                'end_time': now(),
                'duration': int(duration.total_seconds()),
                'pct_gain': pct_gain.round(4),
                'pct_net_gain': pct_net_gain.round(4),
                'pct_slippage': round(pct_total_slippage,3)
            }
        }
    )
    return record['_id']

#------------------------------------------------------------------------------
def snapshot(candle):
    """Gather state of trade--candle, indicators--each tick and save to DB.
    """
    global client
    ob = client.get_orderbook_ticker(symbol=candle['pair'])
    ask = np.float64(ob['askPrice'])
    bid = np.float64(ob['bidPrice'])

    if float(candle['volume']) > 0:
        candle['buy_ratio'] = (candle['buy_vol'] / candle['volume']).round(2)
    else:
        candle['buy_ratio'] = np.float64(0.0)

    df = app.bot.dfc.loc[candle['pair'], strtofreq(candle['freqstr'])]
    dfh, phases = macd.histo_phases(df, candle['pair'], candle['freqstr'], 100)
    dfh['start'] = dfh.index
    dfh['duration'] = dfh['duration'].apply(lambda x: str(x.to_pytimedelta()))
    current = phases[-1].round(3)
    idx = current.index.to_pydatetime()
    current.index = [str(to_local(n.replace(tzinfo=pytz.utc)))[:-10] for n in idx]
    ema_span = len(current)/3 if len(current) >= 3 else len(current)

    return odict({
        'time': now(),
        'price': odict({
            'close': candle['close'],
            'ask': ask,
            'bid': bid,
            'pct_spread': round(pct_diff(bid, ask),3),
            'pct_slippage': round(pct_diff(candle['close'], ask),3)
        }),
        'volume': odict({
            'value': candle['volume']
        }),
        'buyRatio': odict({
            'value': round(candle['buy_ratio'],2)
        }),
        'macd': odict({
            'histo': [{k:v} for k,v in current.to_dict().items()],
            'values': current.values.tolist(),
            'trend': current.diff().ewm(span=ema_span).mean().iloc[-1],
            'desc': current.describe().round(3).to_dict(),
            'history': dfh.to_dict('record')
        }),
        'rsi': signals.rsi(df['close'], 14),
        'orderBook':ob
    })
