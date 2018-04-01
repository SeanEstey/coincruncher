import logging
import numpy as np
import pandas as pd
from pymongo import ReturnDocument
from binance.client import Client
from datetime import timedelta as delta
from app import get_db, strtofreq
from app.common.utils import utc_datetime as now, to_relative_str
from app.common.timer import Timer
import app.bnc
from docs.data import BINANCE
from app.bnc import pct_diff, pairs, candles, printer, strategy

def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)

log = logging.getLogger('trade')

# GLOBALS
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
    app.bnc.dfc = candles.merge_new(pd.DataFrame(), pairs, span=span)

    global client
    client = Client("","")

    log.info('{:,} records loaded in {:,.1f}s.'.format(
        len(app.bnc.dfc), t1.elapsed(unit='s')))

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
    app.bnc.dfc = candles.merge_new(app.bnc.dfc, pairs, span=None)

    tradelog('*'*80)
    duration = to_relative_str(now() - start)
    hdr = "Cycle #{}, Period {} {:>%s}" % (61 - len(str(n_cycles)))
    tradelog(hdr.format(n_cycles, freq_str, duration))
    tradelog('*'*80)
    #tradelog("{} trading pair(s):".format(len(pairs)))
    #[tradelog(x) for x in printer.agg_mkts().to_string().split('\n')]


    # Evaluate Sells
    active = list(db.trades.find({'status':'open'})) #, 'pair':{"$in":pairs}}))

    for trade in active:
        candle = candles.newest(trade['pair'], freq_str, df=app.bnc.dfc)
        result = strategy.evaluate('SELL', candle, record=trade)
        if result:
            if result.get('action') == 'sell':
                trade_ids += [sell(trade, candle, criteria=result)]
            else:
                db.trades.update_one(
                    {"_id": trade["_id"]},
                    {"$push": {"snapshots": result['snapshot']}},
                )

    # Evaluate Buys
    inactive = sorted(list(set(pairs) - set([n['pair'] for n in active])))

    for pair in inactive:
        candle = candles.newest(pair, freq_str, df=app.bnc.dfc)
        result = strategy.evaluate('BUY', candle)
        if result:
            trade_ids += [buy(candle, criteria=result)]

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
    order = {
        'exchange': 'Binance',
        'price': np.float64(orderbook['askPrice']),
        'volume': 1.0,  # FIXME
        'quote': BINANCE['TRADE_AMT'],
        'pct_fee': BINANCE['PCT_FEE'],
        'fee': BINANCE['TRADE_AMT'] * (BINANCE['PCT_FEE']/100),
    }

    criteria['snapshot']['strategy'] = criteria['strategy']
    criteria['details'] = criteria['details']

    return get_db().trades.insert_one({
        'pair': candle['pair'],
        'status': 'open',
        'start_time': now(),
        'snapshots': [criteria['snapshot']],
        'buy': {
            'strategy': criteria['strategy'],
            'details': criteria['details'],
            'time': now(),
            'candle': candle,
            'orderbook': orderbook,
            'order': order
        }
    }).inserted_id

#------------------------------------------------------------------------------
def sell(doc, candle, orderbook=None, criteria=None):
    """Close off existing position and calculate earnings.
    """
    global client
    ob = orderbook if orderbook else client.get_orderbook_ticker(symbol=candle['pair'])
    bid = np.float64(ob['bidPrice'])

    pct_fee = BINANCE['PCT_FEE']
    buy_vol = np.float64(doc['buy']['order']['volume'])
    buy_quote = np.float64(doc['buy']['order']['quote'])
    p1 = np.float64(doc['buy']['order']['price'])

    pct_pdiff = pct_diff(p1, bid)
    quote = (bid * buy_vol) * (1 - pct_fee/100)
    fee = (bid * buy_vol) * (pct_fee/100)

    #net_earn = quote - buy_quote
    pct_net = net_earn = pct_pdiff - (pct_fee*2) #quote - buy_quote
    #pct_net = pct_diff(buy_quote, quote)

    duration = now() - doc['start_time']
    candle['buy_ratio'] = candle['buy_ratio'].round(4)

    get_db().trades.update_one(
        {'_id': doc['_id']},
        {
            '$push': {'snapshots':criteria['snapshot']},
            '$set': {
                'status': 'closed',
                'end_time': now(),
                'duration': int(duration.total_seconds()),
                'pct_pdiff': pct_pdiff.round(4),
                'pct_earn': pct_net.round(4),
                'net_earn': net_earn.round(4),
                'sell': {
                    'time': now(),
                    'candle': candle,
                    'orderbook': ob,
                    'order': {
                        'exchange':'Binance',
                        'price': bid,
                        'volume': 1.0,
                        'quote': quote,
                        'pct_fee': pct_fee,
                        'fee': fee
                    }
                }
            }
        }
    )
    return doc['_id']
