import logging
import numpy as np
import pandas as pd
from collections import OrderedDict as odict
from binance.client import Client
from datetime import timedelta as delta
from docs.conf import binance as _binance, macd_ema, trade_pairs as pairs
from docs.conf import trade_strategies as strategies
from docs.conf import stop_loss
from app import get_db, strtofreq
from app.common.utils import utc_datetime as now, to_relative_str
from app.common.timer import Timer
import app.bot
from app.bot import pct_diff, candles, macd, printer, signals

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

    # Load candle data from DB->dataframe
    log.info('Preloading historic candle data...')
    span = delta(days=7)
    app.bot.dfc = candles.merge_new(pd.DataFrame(), pairs, span=span)
    log.info('{:,} records loaded in {:,.1f}s.'.format(
        len(app.bot.dfc), t1.elapsed(unit='s')))

    print('{} active trading strategies.'.format(len(strategies)))

#------------------------------------------------------------------------------
def update(freqstr):
    """Evaluate Binance market data and execute buy/sell trades.
    """
    global client, n_cycles, freq_str, freq

    client = Client("","")
    _ids=[]
    freq_str = freqstr
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

    _ids += entries(freqstr)
    _ids += exits(freqstr)

    tradelog('-'*80)
    printer.new_trades([n for n in _ids if n])
    tradelog('-'*80)
    printer.positions('open')
    tradelog('-'*80)
    printer.positions('closed')
    n_cycles +=1

#------------------------------------------------------------------------------
def entries(freqstr):
    db = get_db()
    _ids = []
    for pair in pairs:
        c = candles.newest(pair, freqstr)
        ss = snapshot(c)
        for strat in strategies:
            if db.trades.find_one(
                {'status':'open','strategy':strat['name'], 'pair':pair}
            ): continue

            # Entry Filters
            results = [ i(c,ss) for i in strat['entry']['filters'] ]
            if any(i == False for i in results):
                continue

            # Entry Conditions
            results = [ i(c,ss) for i in strat['entry']['conditions'] ]
            if any(i == True for i in results):
                _ids.append(buy(c, strat['name'], ss))
    return _ids

#------------------------------------------------------------------------------
def exits(freqstr):
    db = get_db()
    _ids = []
    for trade in db.trades.find({'status':'open', 'freq':freqstr}):
        candle = candles.newest(trade['pair'], freqstr)
        conf = strategies[[n['name'] for n in strategies].index(trade['strategy'])]
        ss = snapshot(candle)

        # Must pass all user-defined exit filters
        results = [ n(candle, ss, trade) for n in conf['exit']['filters']]
        if any(n == False for n in results):
            continue

        # Sell on meeting any exit condition
        results = [ n(candle, ss, trade) for n in conf['exit']['conditions']]
        # Stop loss
        results.append(
            candle['close'] < trade['orders'][0]['candle']['close']*(1-stop_loss)
        )

        if any(n == True for n in results):
            _ids.append(sell(trade, candle, ss))
        else:
            db.trades.update_one(
                {"_id":trade["_id"]}, {"$push": {"snapshots":ss}}
            )
    return _ids

#------------------------------------------------------------------------------
def buy(candle, strat_name, ss):
    """Create or update existing position for zscore above threshold value.
    """
    global client
    orderbook = client.get_orderbook_ticker(symbol=candle['pair'])

    return get_db().trades.insert_one(odict({
        'pair': candle['pair'],
        'freq': candle['freq'],
        'status': 'open',
        'start_time': now(),
        'strategy': strat_name,
        'snapshots': [ss],
        'orders': [odict({
            'action':'BUY',
            'ex': 'Binance',
            'time': now(),
            'price': candle['close'], # np.float64(orderbook['askPrice']),
            'volume': 1.0,
            'quote': _binance['trade_amt'],
            'fee': _binance['trade_amt'] * (_binance['pct_fee']/100),
            'orderbook': orderbook,
            'candle': candle
        })]
    })).inserted_id

#------------------------------------------------------------------------------
def sell(record, candle, ss):
    """Close off existing position and calculate earnings.
    """
    bid = np.float64(ss['orderBook']['bidPrice'])

    pct_fee = _binance['pct_fee']
    buy_vol = np.float64(record['orders'][0]['volume'])
    buy_quote = np.float64(record['orders'][0]['quote'])
    p1 = np.float64(record['orders'][0]['price'])

    pct_gain = pct_diff(p1, candle['close'])
    quote = buy_quote * (1 - pct_fee/100)
    fee = (bid * buy_vol) * (pct_fee/100)
    pct_net_gain = net_earn = pct_gain - (pct_fee*2)

    duration = now() - record['start_time']
    candle['buy_ratio'] = candle['buy_ratio'].round(4)

    get_db().trades.update_one(
        {'_id': record['_id']},
        {
            '$push': {'snapshots':ss},
            '$push': {
                'orders': odict({
                    'action': 'SELL',
                    'ex': 'Binance',
                    'time': now(),
                    'price': candle['close'],
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
            }
        }
    )
    return record['_id']

#------------------------------------------------------------------------------
def snapshot(candle):
    """Gather state of trade--candle, indicators--each tick and save to DB.
    """
    z = signals.z_score(candle, 25).to_dict()
    client = Client("","")
    ob = client.get_orderbook_ticker(symbol=candle['pair'])

    macd_desc = macd.describe(candle, ema=macd_ema)
    phase = macd_desc['phase']
    # Convert datetime index to str for mongodb storage.
    phase.index = [ str(n)[:-10] for n in phase.index.values ]
    last = phase.iloc[-1]

    print('{} {} {}'.format(
        candle['pair'], candle['freq'], macd_desc['details']))

    return odict({
        'time': now(),
        #'strategy': None,
        'details': macd_desc['details'],
        'price': odict({
            'close': candle['close'],
            'z-score': round(z['close'], 2),
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
        }),
        'orderBook':ob
    })

#-------------------------------------------------------------------------------
def summarize():
    """Performance summary of trades, grouped by day/strategy.
    """
    results = db.trades.aggregate([
        { '$match': {
            'status':'closed'
        }},
        { '$group': {
            '_id': {'strategy':'$strategy', 'day': {'$dayOfYear':'$end_time'}},
            'totalGain': {'$sum':'$pct_net_gain'},
            'count': {'$sum': 1}
        }}
    ])
    print(results)
