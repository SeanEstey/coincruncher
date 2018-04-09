import logging
import numpy as np
import pandas as pd
from collections import OrderedDict as odict
from binance.client import Client
from datetime import timedelta as delta
from docs.conf import binance as _binance, trade_pairs as pairs
from docs.conf import trade_strategies as strategies
from app import get_db, strtofreq
from app.common.utils import utc_datetime as now, to_relative_str
from app.common.timer import Timer
import app.bot
from app.bot import pct_diff, candles, macd, printer, signals, strategy

def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)

# GLOBALS
log = logging.getLogger('trade')
n_cycles = 0
start = now()
freq = None
freq_str = None
client = None
strats = []

#-------------------------------------------------------------------------------
def add_strat(name):
    """Each strats element is a dict of keys: 'name', 'conf', 'callback'
    """
    global strats

    names = [ n['name'] for n in strategies ]
    conf = strategies[names.index(name)]

    if conf['callback']['str_func'].index('strategy.') == -1:
        print("strat callback func must be in app.bot.strategy module")
        return
    else:
        func = conf['callback']['str_func'].split('.')[-1]
        strats.append({
            'name':conf['name'],
            'conf':conf,
            'callback':getattr(strategy, func)
        })

#------------------------------------------------------------------------------
def rmv_strat(name):
    global strats
    del strats[[ n['name'] for n in strats].index(name)]

#------------------------------------------------------------------------------
def get_strat(name):
    return strats[[ n['name'] for n in strats ].index(name)]

#------------------------------------------------------------------------------
def init(strat_names=None):
    """Preload candles records from mongoDB to global dataframe.
    Performance: ~3,000ms/100k records
    """
    if strat_names:
        [add_strat(n) for n in strat_names]
        print('Loaded {} trading strategies.'.format(len(strats)))
        print('Active strategies: %s' % strats)

    t1 = Timer()
    log.info('Preloading historic candle data...')

    span = delta(days=7)
    app.bot.dfc = candles.merge_new(pd.DataFrame(), pairs, span=span)

    global client
    client = Client("","")

    log.info('{:,} records loaded in {:,.1f}s.'.format(
        len(app.bot.dfc), t1.elapsed(unit='s')))

#------------------------------------------------------------------------------
def update(_freq_str):
    """Evaluate Binance market data and execute buy/sell trades.
    # TODO: add back in:
    # Any active strategies trading on this freq?
    # if freq_str in trade_freq:
    #    siglog('-'*80)
    #    for pair in pairs:
    #        candles.describe(candles.newest(
    #           pair, freq_str, df=app.bot.dfc))
    """
    global n_cycles, freq_str, freq

    _ids=[]
    freq_str = _freq_str
    freq = strtofreq[freq_str]
    t1 = Timer()
    db = get_db()
    my_open = list(db.trades.find({'status':'open', 'freq':freq_str}))

    # Update candles updated by websocket
    app.bot.dfc = candles.merge_new(app.bot.dfc, pairs, span=None)

    tradelog('*'*80)
    duration = to_relative_str(now() - start)
    hdr = "Cycle #{}, Period {} {:>%s}" % (61 - len(str(n_cycles)))
    tradelog(hdr.format(n_cycles, freq_str, duration))
    tradelog('*'*80)

    # Manage open positions
    for trade in my_open:
        candle = candles.newest(trade['pair'], freq_str, df=app.bot.dfc)
        strat = get_strat(trade['strategy'])
        ss = snapshot(candle, strat['conf'])
        callback = strat['callback']
        result = callback(candle, ss, conf=strat['conf'], record=trade)
        ss['details'] += result.get('details','')

        if result['action'] == 'SELL':
            print("sell details: %s" % result['details'])
            _ids.append(sell(trade, candle, ss))
        else:
            db.trades.update_one({"_id":trade["_id"]},
                {"$push": {"snapshots":ss}})

        print('{} {} {}'.format(
            candle['pair'], candle['freq'], ss['details']))

    # Manage new positions
    inactive = sorted(list(
        set(pairs)-set([n['pair'] for n in my_open])
    ))

    for pair in inactive:
        candle = candles.newest(pair, freq_str, df=app.bot.dfc)

        for n in strats:
            # Skip if callback doesn't subscribe to current frequency
            if freq_str not in n['conf']['callback']['freq']:
                continue

            ss = snapshot(candle, n['conf'])
            callback = n['callback']
            result = callback(candle, ss, conf=n['conf'])

            if result['action'] == 'BUY':
                _ss = ss.copy()
                _ss['details'] += result.get('details','')
                _ids.append(buy(candle, n['name'], _ss))

            print('{} {} {}'.format(
                candle['pair'], candle['freq'], ss.get('details','')))

    tradelog('-'*80)
    printer.new_trades([n for n in _ids if n])
    tradelog('-'*80)
    printer.positions('open')
    tradelog('-'*80)
    printer.positions('closed')

    n_cycles +=1

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
        #'strat_conf': get_strat(criteria['snapshot']['strategy'])['conf'],
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
def snapshot(candle, conf):
    """Gather state of trade--candle, indicators--each tick and save to DB.
    """
    z = signals.z_score(candle, 25).to_dict() #strats['z-score']['periods']).to_dict()
    client = Client("","")
    ob = client.get_orderbook_ticker(symbol=candle['pair'])

    macd_desc = macd.describe(candle, ema=conf.get('ema'))
    phase = macd_desc['phase']
    # Convert datetime index to str for mongodb storage.
    phase.index = [ str(n)[:-10] for n in phase.index.values ]
    last = phase.iloc[-1]

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
