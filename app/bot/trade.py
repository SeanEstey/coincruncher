import logging
import numpy as np
import pandas as pd
from pymongo import ReplaceOne
from collections import OrderedDict as odict
from binance.client import Client
from datetime import timedelta as delta, datetime
from docs.conf import binance as _binance, macd_ema
from docs.botconf import trade_pairs as pairs, strategies
from app import get_db
from app.common.timeutils import strtofreq
from app.common.utils import utc_datetime as now, to_relative_str
from app.common.timer import Timer
import app.bot
from app.bot import pct_diff, candles, macd, printer, signals

def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)

# GLOBALS
log = logging.getLogger('trade')
logwidth = 60
n_cycles = 0
start = now()
freq = None
freq_str = None
client = None
snapshots = {}

#------------------------------------------------------------------------------
def init():
    """Preload candles records from mongoDB to global dataframe.
    Performance: ~3,000ms/100k records
    """
    t1 = Timer()
    print('Loading candles...')
    app.bot.dfc = candles.load(pairs)

    # Lookup/store Binance asset metadata for active trading pairs.
    client = Client('','')
    info = client.get_exchange_info()
    meta = [n for n in info['symbols'] if n['symbol'] in pairs]
    ops = [ ReplaceOne({'symbol':n['symbol']}, n, upsert=True) for n in meta ]
    get_db().meta.bulk_write(ops)

    # TODO: Determine all freqs required in trading algo's, return
    # list to daemon to setup timers for each.
    print('{} active trading strategies.'.format(len(strategies)))

    print('Initialized in {} ms. Ready to trade.'.format(t1.elapsed()))

#------------------------------------------------------------------------------
def update(freqstr):
    """Evaluate Binance market data and execute buy/sell trades.
    """
    global client, n_cycles, freq_str, freq, snapshots
    # One Binance client instance per update cycle
    client = Client("","")
    _ids=[]
    freq_str = freqstr
    freq = strtofreq(freq_str)
    t1 = Timer()
    db = get_db()

    # Merge new candle data
    tradelog('-'*logwidth)
    app.bot.dfc = candles.load(pairs,
        freqstr=freqstr,
        startstr="{} seconds ago utc".format(freq),
        dfm=app.bot.dfc)

    snapshots = {}
    for pair in pairs:
        candle = candles.to_dict(pair, freqstr)
        snapshots[pair] = snapshot(candle)

    tradelog('*'*logwidth)
    duration = to_relative_str(now() - start)
    hdr = "Cycle #{}, Period {} {:>%s}" % (61 - len(str(n_cycles)))
    tradelog(hdr.format(n_cycles, freq_str, duration))
    tradelog('*'*logwidth)

    _ids += exits(freqstr)
    _ids += entries(freqstr)

    tradelog('-'*logwidth)
    printer.new_trades([n for n in _ids if n])
    tradelog('-'*logwidth)
    printer.positions(freqstr)
    tradelog('-'*logwidth)
    tradelog('-'*logwidth)
    earnings()
    n_cycles +=1

#------------------------------------------------------------------------------
def entries(freqstr):
    db = get_db()
    _ids = []
    for pair in pairs:
        c = candles.to_dict(pair, freqstr)
        ss = snapshots[pair]
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
            if all(i == True for i in results):
                _ids.append(buy(c, strat['name'], ss))
    return _ids

#------------------------------------------------------------------------------
def exits(freqstr):
    db = get_db()
    _ids = []
    for trade in db.trades.find({'status':'open'}): #'freq':freqstr}):
        candle = candles.to_dict(trade['pair'], freqstr)
        conf = strategies[[n['name'] for n in strategies].index(trade['strategy'])]
        ss = snapshots[trade['pair']]

        # Stop loss
        if candle['freq'] in conf['stop_loss']['freq']:
            pct = pct_diff(trade['orders'][0]['candle']['close'], candle['close'])
            if pct < conf['stop_loss']['pct']:
                print("STOP LOSS {} {}".format(candle['freq'], trade['pair']))
                _ids.append(sell(trade, candle, ss))
                continue

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
def buy(candle, strat_name, ss):
    """Create or update existing position for zscore above threshold value.
    """
    global client
    orderbook = client.get_orderbook_ticker(symbol=candle['pair'])
    bid = np.float64(orderbook['bidPrice'])
    ask = np.float64(orderbook['askPrice'])

    db = get_db()
    meta = db.meta.find_one({'symbol':candle['pair']})

    result = db.trades.insert_one(odict({
        'pair': candle['pair'],
        'quote_asset': meta['quoteAsset'],
        'freq': candle['freq'],
        'status': 'open',
        'start_time': now(),
        'strategy': strat_name,
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

    print("BUY {} ({})".format(candle['pair'], strat_name))
    return result.inserted_id

#------------------------------------------------------------------------------
def sell(record, candle, ss):
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

    print("SELL {} ({})".format(candle['pair'], record['strategy']))

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
    #z = signals.z_score(candle, 25).to_dict()
    ob = client.get_orderbook_ticker(symbol=candle['pair'])
    ask = np.float64(ob['askPrice'])
    bid = np.float64(ob['bidPrice'])

    dfh, phases = macd.histo_phases(app.bot.dfc,
        candle['pair'], candle['freq'], 100)
    phase = phases[-1]

    #macd_desc = macd.describe(candle, ema=macd_ema)
    #phase = macd_desc['phase']
    # Convert datetime index to str for mongodb storage.
    phase.index = [ str(n)[:-10] for n in phase.index.values ]
    last = phase.iloc[-1]







    print('{} {} {}'.format(
        candle['pair'], candle['freq'], macd_desc['details']))

    return odict({
        'time': now(),
        'details': macd_desc['details'],
        'price': odict({
            'close': candle['close'],
            #'z-score': round(z['close'], 2),
            'ask': ask,
            'bid': bid,
            'pct_spread': round(pct_diff(bid, ask),3),
            'pct_slippage': round(pct_diff(candle['close'], ask),3)
        }),
        'volume': odict({
            'value': candle['volume'],
            #'z-score': round(z['volume'],2),
        }),
        'buyRatio': odict({
            'value': round(candle['buy_ratio'],2),
            #'z-score': round(z['buy_ratio'], 2),
        }),
        'macd': odict({
            'value': last.round(3),
            'trend': macd_desc['trend'],
            'phases': phase.round(3).to_dict(odict),
            'desc': phase.describe().round(3).to_dict()
        }),
        'orderBook':ob
    })

#-------------------------------------------------------------------------------
def earnings():
    """Performance summary of trades, grouped by day/strategy.
    """
    from pprint import pprint
    db = get_db()

    gain = list(db.trades.aggregate([
        {'$match': {'status':'closed', 'pct_net_gain':{'$gte':0}}},
        {'$group': {
            '_id': {'strategy':'$strategy', 'day': {'$dayOfYear':'$end_time'}},
            'total': {'$sum':'$pct_net_gain'},
            'count': {'$sum': 1}
        }}
    ]))
    loss = list(db.trades.aggregate([
        {'$match': {'status':'closed', 'pct_net_gain':{'$lt':0}}},
        {'$group': {
            '_id': {'strategy':'$strategy', 'day': {'$dayOfYear':'$end_time'}},
            'total': {'$sum':'$pct_net_gain'},
            'count': {'$sum': 1}
        }}
    ]))
    assets = list(db.trades.aggregate([
        { '$match': {'status':'closed', 'pct_net_gain':{'$gte':0}}},
        { '$group': {
            '_id': {
                'asset':'$quote_asset',
                'day': {'$dayOfYear':'$end_time'}},
            'total': {'$sum':'$pct_net_gain'},
            'count': {'$sum': 1}
        }}
    ]))

    today = int(datetime.utcnow().strftime('%j'))
    gain = [ n for n in gain if n['_id']['day'] == today]
    loss = [ n for n in loss if n['_id']['day'] == today]
    #today_by_asset = [ n for n in assets if n['_id']['day'] == day_of_yr]

    for n in gain:
        tradelog("{:} today: {:} wins ({:+.2f}%)."\
            .format(
                n['_id']['strategy'],
                n['count'],
                n['total']
            ))
    for n in loss:
        tradelog("{:} today: {:} losses ({:+.2f}%)."\
            .format(
                n['_id']['strategy'],
                n['count'],
                n['total']
            ))

    #ratio = (n_win/len(closed))*100 if len(closed) >0 else 0
    #duration = to_relative_str(now() - start)
    #tradelog("{:+.2f}% net profit today.".format(pct_net_gain))
    #tradelog('{:+.2f}% paid in slippage.'.format(pct_slip))
    #tradelog('{:+.2f)% paid in fees.'.format(pct_fees))

    return (gain, loss, assets)
