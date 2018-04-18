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
from docs.conf import binance as _binance, macd_ema
from docs.botconf import strategies as strats, tradefreqs
from app import get_db
from app.common.timeutils import strtofreq
from app.common.utils import to_local, utc_datetime as now, to_relative_str
from app.common.timer import Timer
import app.bot
from app.bot import pct_diff, candles, macd, reports, signals

def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)

# GLOBALS
log = logging.getLogger('trade')
logwidth = 50
n_cycles = 0
start = now()
client = None

#---------------------------------------------------------------------------
def run():
    from main import q_closed
    global client
    n_cycles = 0
    db = get_db()
    client = Client('','')

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

        time.sleep(5)

#------------------------------------------------------------------------------
def stoploss():
    """consume from q_open and sell any trades if price falls below stop
    loss threshold.
    """
    from main import q_open
    db = get_db()

    while True:
        while q_open.empty() == False:
            c = q_open.get()
            query = {'pair':c['pair'], 'freq':c['freqstr'], 'status':'open'}

            for trade in db.trades.find(query):
                diff = pct_diff(trade['orders'][0]['candle']['close'], c['close'])

                if diff < trade['stoploss']:
                    sell(trade, c, snapshot(c), details='Stop Loss')

        print('stoploss queue cleared.')
        time.sleep(10)

#------------------------------------------------------------------------------
def _old():
    # FIXME: put on a thread timer
    # Merge new candle data
    app.bot.dfc = candles.load(pairs, [freqstr],
        startstr="{} seconds ago utc".format(freq*3),
        dfm=app.bot.dfc)

    tradelog('*'*logwidth)
    duration = to_relative_str(now() - start)
    hdr = "Cycle #{}, Period {} {:>%s}" % (31 - len(str(n_cycles)))
    tradelog(hdr.format(n_cycles, freq_str, duration))
    tradelog('-'*logwidth)

#------------------------------------------------------------------------------
def init():
    t1 = Timer()
    # Lookup/store Binance asset metadata for active trading pairs.
    client = Client('','')
    info = client.get_exchange_info()

    ops = [UpdateOne({'symbol':n['symbol']}, {'$set':n}, upsert=True) for n in info['symbols']]
    get_db().assets.bulk_write(ops)

    print("{} available trading pairs retrieved from api.".format(len(ops)))

    enabled = app.get_db().assets.find({'botTradeStatus':'ENABLED'})

    if enabled.count() == 0:
        raise Exception("No tradepairs enabled")

    pairs = [ n['symbol'] for n in list(enabled) ]

    print("{} pairs meet requirements and have been enabled.".format(enabled.count()))
    print(pairs)
    print('{} active trading strategies.'.format(len(strats)))

    app.bot.dfc = candles.load(pairs, [])

    for pair in pairs:
        for freqstr in tradefreqs:
            if (pair,freqstr) not in app.bot.dfc.index:
                print("Querying ({},{}) data..."\
                    .format(pair, freqstr))
                candles.update([pair], freqstr,
                    start="72 hours ago utc", force=True)
                app.bot.dfc = candles.load(pairs, [freqstr],
                    dfm=app.bot.dfc)

    print('Initialized in {:.0f} ms.'.format(t1.elapsed()))
    print('Ready to trade, sir!')

#------------------------------------------------------------------------------
def get_enabled_pairs():
    enabled = app.get_db().assets.find({'botTradeStatus':'ENABLED'})
    return [ n['symbol'] for n in list(enabled) ]

#------------------------------------------------------------------------------
def enable_pairs(authlist):
    """To disable all pairs/freq, pass in empty list.
    """
    db = app.get_db()

    result = db.meta.update_many({},
        {'$set':{'botTradeStatus':'DISABLED', 'botTradeFreq':[]}})

    print("Reset {}/{} pairs.".format(
        result.modified_count,
        db.meta.find({'botTradeStatus':'ENABLED'}).count()
    ))

    ops = [
        UpdateOne({'symbol':auth['pair']},
            {'$set':{'botTradeStatus':'ENABLED'},
            '$push':{'botTradeFreq':auth['freq']}},
            upsert=True) for auth in authlist
    ]
    if len(ops) > 0:
        result = db.meta.bulk_write(ops)
        print("{} pairs updated, {} upserted.".format(
            result.modified_count, result.upserted_count))

#------------------------------------------------------------------------------
def eval_entries(candle, ss):
    db = get_db()
    ids = []

    for strat in strats:
        if db.trades.find_one(
            {'status':'open','strategy':strat['name'], 'pair':pair}
        ): continue

        # Entry Filters
        results = [ i(candle,ss) for i in strat['entry']['filters'] ]
        if any(i == False for i in results):
            continue

        # Entry Conditions
        results = [ i(candle,ss) for i in strat['entry']['conditions'] ]
        if all(i == True for i in results):
            ids.append(buy(candle, strat['name'], ss))
    return ids

#------------------------------------------------------------------------------
def eval_exits(candle, ss):
    db = get_db()
    _ids = []
    for trade in db.trades.find({'status':'open'}): #'freq':freqstr}):
        candle = candles.to_dict(trade['pair'], freqstr)
        conf = strats[[n['name'] for n in strats].index(trade['strategy'])]
        ss = snapshots[trade['pair']]


        # Evaluate exit conditions if all filters passed.
        conditions = []
        filt = [ n(candle, ss, trade) for n in conf['exit']['filters']]
        if all(n == True for n in filt):
            conditions += [ n(candle, ss, trade) for n in conf['exit']['conditions']]

            if all(n == True for n in conditions):
                print("SELL CONDITION(S) TRUE {} {}".format(candle['freq'], trade['pair']))
                _ids.append(sell(trade, candle, ss))
            else:
                db.trades.update_one(
                    {"_id":trade["_id"]}, {"$push": {"snapshots":ss}}
                )
    return _ids

#------------------------------------------------------------------------------
def buy(candle, strategy, ss):
    """Create or update existing position for zscore above threshold value.
    """
    global client
    db = get_db()
    orderbook = client.get_orderbook_ticker(symbol=candle['pair'])
    bid = np.float64(orderbook['bidPrice'])
    ask = np.float64(orderbook['askPrice'])
    meta = db.assets.find_one({'symbol':candle['pair']})
    stratconf = strats[[n['name'] for n in strats].index(strategy)]

    result = db.trades.insert_one(odict({
        'pair': candle['pair'],
        'quote_asset': meta['quoteAsset'],
        'freq': candle['freqstr'],
        'status': 'open',
        'start_time': now(),
        'strategy': strategy,
        'stoploss': stratconf['stoploss'],
        'snapshots': [ss],
        'orders': [odict({
            'action':'BUY',
            'ex': 'Binance',
            'time': now(),
            'price': ask,
            'pct_spread': pct_diff(bid, ask),
            'pct_slippage': pct_diff(candle['close'], ask),
            'volume': 1.0,
            'quote': _binance['trade_amt'],
            'fee': _binance['trade_amt'] * (_binance['pct_fee']/100),
            'orderbook': orderbook,
            'candle': candle
        })]
    }))

    print("BUY {} ({})".format(candle['pair'], strategy))
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

    pct_fee = _binance['pct_fee']
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
        .format(candle['pair'], record['strategy'], details))

    get_db().trades.update_one(
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
