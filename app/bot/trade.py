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
from app.bot import candles, macd, reports, signals
from app.common.timeutils import strtofreq
from app.common.utils import pct_diff, to_local, utc_datetime as now, to_relative_str
from app.common.timer import Timer

def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)
log = logging.getLogger('trade')
start = now()
n_cycles = 0

#---------------------------------------------------------------------------
def run(e_pairs):
    from main import q_closed
    global n_cycles

    client = app.bot.client
    db = app.db
    n_cycles = 0

    while True:
        exited, entered = None, None
        n=0

        if q_closed.empty() == False:
            n_cycles += 1
            tradelog('*'*TRADELOG_WIDTH)
            duration = to_relative_str(now() - start)
            hdr = "Cycle #{} {:>%s}" % (31 - len(str(n_cycles)))
            tradelog(hdr.format(n_cycles, duration))
            tradelog('-'*TRADELOG_WIDTH)

        while q_closed.empty() == False:
            candle = q_closed.get()
            ss = snapshot(candle)
            db.ss.insert_one({**{'pair':candle['pair']},**ss})
            exited = eval_exit(candle, ss)
            entered = eval_entry(candle, ss)
            n+=1

        if n > 0:
            print('{} queue candles cleared.'.format(n))
            if entered:
                reports.trades(entered)

            #if db.trades.find({'status':'open'}).count() > 0:
            reports.positions()

            if entered or exited:
                reports.earnings()
        else:
            print('closed candle queue empty.')

        #print("app.bot.dfc.lenth={}.".format(len(app.bot.dfc)))
        time.sleep(10)

#------------------------------------------------------------------------------
def stoploss(e_pairs):
    """consume from q_open and sell any trades if price falls below stop
    loss threshold.
    """
    from main import q_open
    db = app.get_db()

    while True:
        n=0
        while q_open.empty() == False:
            c = q_open.get()
            query = {'pair':c['pair'], 'freqstr':c['freqstr'], 'status':'open'}

            for trade in db.trades.find(query):
                diff = pct_diff(trade['orders'][0]['candle']['close'], c['close'])

                if diff < trade['stoploss']:
                    sell(trade, c, snapshot(c), details='Stop Loss')
            n+=1

        print('{} stoploss queue items cleared.'.format(n))
        time.sleep(10)

#------------------------------------------------------------------------------
def eval_entry(candle, ss):
    db = app.get_db()
    ids = []
    for algo in TRADE_ALGOS:
        if db.trades.find_one(
            {'freqstr':candle['freqstr'], 'algo':algo['name'], 'status':'open'}):
            continue

        # Test all filters/conditions eval to True
        if all([fn(candle,ss) for fn in algo['entry']['filters']]):
            if all([fn(candle,ss) for fn in algo['entry']['conditions']]):
                ids.append(buy(candle, algo, ss))
    return ids

#------------------------------------------------------------------------------
def eval_exit(candle, ss):
    db = app.get_db()
    ids = []
    query = {
        'pair':candle['pair'],
        'freqstr':candle['freqstr'],
        'status':'open'
    }
    for trade in db.trades.find(query):
        algo = [n for n in TRADE_ALGOS if n['name'] == trade['algo']][0]

        # Test all filters/conditions eval to True
        if all([fn(candle, ss, trade) for fn in algo['exit']['filters']]):
            if all([fn(candle, ss, trade) for fn in algo['exit']['conditions']]):
                ids.append(sell(trade, candle, ss,
                    details="Algo filters/conditions met"))
            else:
                db.trades.update_one(
                    {"_id":trade["_id"]},
                    {"$push": {"snapshots":ss}})


    return ids

#------------------------------------------------------------------------------
def buy(candle, algoconf, ss):
    """Create or update existing position for zscore above threshold value.
    """
    client = app.bot.client
    db = app.db
    orderbook = client.get_orderbook_ticker(symbol=candle['pair'])
    bid = np.float64(orderbook['bidPrice'])
    ask = np.float64(orderbook['askPrice'])
    meta = db.assets.find_one({'symbol':candle['pair']})

    result = db.trades.insert_one(odict({
        'pair': candle['pair'],
        'quote_asset': meta['quoteAsset'],
        'freqstr': candle['freqstr'],
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
    client = app.bot.client
    db = app.db

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
    client = app.bot.client
    db = app.db

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
