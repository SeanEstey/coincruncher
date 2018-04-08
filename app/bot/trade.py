import logging
import numpy as np
import pandas as pd
from collections import OrderedDict as odict
from binance.client import Client
from datetime import timedelta as delta
from docs.conf import binance as _conf, trading_pairs as pairs,\
candle_freq, strategies as strats
from app import get_db, strtofreq
from app.common.utils import utc_datetime as now, to_relative_str
from app.common.timer import Timer
import app.bot
from app.bot import pct_diff, candles, macd, printer, strategy

def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)

# GLOBALS
log = logging.getLogger('trade')
n_cycles = 0
start = now()
freq = None
freq_str = None
client = None

#------------------------------------------------------------------------------
def init():
    """Preload candles records from mongoDB to global dataframe.
    Performance: ~3,000ms/100k records
    """
    t1 = Timer()
    log.info('Preloading historic data...')

    span = delta(days=7)
    app.bot.dfc = candles.merge_new(pd.DataFrame(), pairs, span=span)

    global client
    client = Client("","")

    log.info('{:,} records loaded in {:,.1f}s.'.format(
        len(app.bot.dfc), t1.elapsed(unit='s')))

#------------------------------------------------------------------------------
def update(_freq_str):
    """Evaluate Binance market data and execute buy/sell trades.
    """
    global n_cycles, freq_str, freq

    trade_ids=[]
    freq_str = _freq_str
    freq = strtofreq[freq_str]
    t1 = Timer()
    db = get_db()

    # Update candles updated by websocket
    app.bot.dfc = candles.merge_new(app.bot.dfc, pairs, span=None)

    tradelog('*'*80)
    duration = to_relative_str(now() - start)
    hdr = "Cycle #{}, Period {} {:>%s}" % (61 - len(str(n_cycles)))
    tradelog(hdr.format(n_cycles, freq_str, duration))
    tradelog('*'*80)

    # Output candle signals to siglog
    if freq_str in candle_freq:
        siglog('-'*80)
        for pair in pairs:
            printer.candle_sig(candles.newest(pair, freq_str, df=app.bot.dfc))

    # Evaluate existing positions
    active = list(db.trades.find({'status':'open', 'freq':freq_str}))

    for trade in active:
        candle = candles.newest(trade['pair'], freq_str, df=app.bot.dfc)
        result = strategy.update(candle, trade)

        print('{} {} {}'.format(
            candle['pair'], candle['freq'], result['snapshot']['details']))

        if result['action'] == 'SELL':
            trade_ids += [sell(trade, candle, criteria=result)]
        else:
            db.trades.update_one({"_id": trade["_id"]},
                {"$push": {"snapshots": result['snapshot']}})

    # Inverse active list and evaluate opening new positions
    inactive = sorted(list(set(pairs) - set([n['pair'] for n in active])))

    for pair in inactive:
        candle = candles.newest(pair, freq_str, df=app.bot.dfc)
        results = strategy.evaluate(candle)
        for res in results:
            print('{} {} {}'.format(
                candle['pair'], candle['freq'], res['snapshot']['details']))

            if res['action'] == 'BUY':
                trade_ids += [buy(candle, criteria=res)]

    tradelog('-'*80)
    printer.new_trades([n for n in trade_ids if n])
    tradelog('-'*80)
    printer.positions('open')
    tradelog('-'*80)
    printer.positions('closed')

    n_cycles +=1

#------------------------------------------------------------------------------
def buy(candle, criteria):
    """Create or update existing position for zscore above threshold value.
    """
    global client
    orderbook = client.get_orderbook_ticker(symbol=candle['pair'])

    return get_db().trades.insert_one(odict({
        'pair': candle['pair'],
        'freq': candle['freq'],
        'status': 'open',
        'start_time': now(),
        'strategy': criteria['snapshot']['strategy'],
        'snapshots': [
            criteria['snapshot']
        ],
        'orders': [odict({
            'action':'BUY',
            'ex': 'Binance',
            'time': now(),
            'price': candle['close'], # np.float64(orderbook['askPrice']),
            'volume': 1.0,
            'quote': _conf['trade_amt'],
            'fee': _conf['trade_amt'] * (_conf['pct_fee']/100),
            'orderbook': orderbook,
            'candle': candle
        })]
    })).inserted_id

#------------------------------------------------------------------------------
def sell(doc, candle, orderbook=None, criteria=None):
    """Close off existing position and calculate earnings.
    """
    global client
    ob = orderbook if orderbook else client.get_orderbook_ticker(symbol=candle['pair'])
    bid = np.float64(ob['bidPrice'])

    pct_fee = _conf['pct_fee']
    buy_vol = np.float64(doc['orders'][0]['volume'])
    buy_quote = np.float64(doc['orders'][0]['quote'])
    p1 = np.float64(doc['orders'][0]['price'])

    pct_gain = pct_diff(p1, candle['close'])
    quote = buy_quote * (1 - pct_fee/100)
    fee = (bid * buy_vol) * (pct_fee/100)
    pct_net_gain = net_earn = pct_gain - (pct_fee*2) #quote - buy_quote

    duration = now() - doc['start_time']
    candle['buy_ratio'] = candle['buy_ratio'].round(4)

    get_db().trades.update_one(
        {'_id': doc['_id']},
        {
            '$push': {'snapshots':criteria['snapshot']},
            '$push': {
                'orders': odict({
                    'action': 'SELL',
                    'ex': 'Binance',
                    'time': now(),
                    'price': candle['close'],
                    'volume': 1.0,
                    'quote': buy_quote,
                    'fee': fee,
                    'orderbook': ob,
                    'candle': candle,
                })
            },
            '$set': {
                'status': 'closed',
                'end_time': now(),
                'duration': int(duration.total_seconds()),
                'pct_gain': pct_gain.round(4),
                'pct_net_gain': pct_net_gain.round(4),
            }
        }
    )
    return doc['_id']

#------------------------------------------------------------------------------
def snapshot(candle):
    """Gather state of trade--candle, indicators--each tick and save to DB.
    """
    z = signals.z_score(candle, strats['z-score']['periods']).to_dict()
    client = Client("","")
    ob = client.get_orderbook_ticker(symbol=candle['pair'])

    macd_desc = macd.describe(candle)
    phase = macd_desc['phase']
    # Convert datetime index to str for mongodb storage.
    phase.index = [ str(n)[:-10] for n in phase.index.values ]
    last = phase.iloc[-1]

    return odict({
        'time': now(),
        'strategy': None,
        'details': macd_desc['details'],
        'price': odict({
            'close': candle['close'],
            'z-score': round(z['close'], 2),
            'emaDiff': signals.ema_pct_change(
                candle, strats['ema']['span']).iloc[-1],
            'ask': float(ob['askPrice']),
            'bid': float(ob['bidPrice'])
        }),
        'volume': odict({
            'value': candle['volume'],
            'z-score': round(z['volume'],2),
        }),
        'buyRatio': odict({
            'value': round(candle['buy_ratio'],2),
            'z-score': round(z['buy_ratio'], 2),
        }),
        'macd': odict({
            'value': last.round(10),
            'phase': phase.round(10).to_dict(odict),
            'desc': phase.describe().round(10).to_dict()
        })
    })
