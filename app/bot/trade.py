# app.bot.trade
import time
import logging
import pytz
import numpy as np
import pandas as pd
from decimal import Decimal
from pprint import pprint
from pymongo import ReplaceOne, UpdateOne
from collections import OrderedDict as odict
from binance.client import Client
from datetime import timedelta as delta, datetime
from docs.conf import *
from docs.botconf import *
import app, app.bot
from app.bot import get_pairs, candles, macd, reports, signals
from app.common.timeutils import strtofreq
from app.common.utils import pct_diff, to_local, utc_datetime as now, to_relative_str
from app.common.timer import Timer

def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)
log = logging.getLogger('trade')
# Track every trade price for open position pairs
tradedata = {}
start = now()
n_cycles = 0

#---------------------------------------------------------------------------
def full_klines(e_pairs):
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
            print('{} closed_candle queue items cleared.'.format(n))
            if entered:
                reports.new_trades(entered)
                tradelog('-'*TRADELOG_WIDTH)

            reports.positions()

            if entered or exited:
                reports.earnings()

        time.sleep(10)

#------------------------------------------------------------------------------
def part_klines(e_pairs):
    """consume items from open candle queue. Track trade prices for open
    # positions and invoke stop losses when necessary.
    """
    from main import q_open
    global tradedata
    db = app.get_db()

    while True:
        items = []
        while q_open.empty() == False:
            candle = q_open.get()
            items.append(candle)
            query = {'pair':candle['pair'], 'freqstr':candle['freqstr'], 'status':'open'}

            for trade in db.trades.find(query):
                diff = pct_diff(trade['snapshots'][0]['candle']['close'], candle['close'])

                if diff < trade['stoploss']:
                    sell(trade, candle, snapshot(candle), details='Stop Loss')

        # Update global trade price tracker list
        if len(items) > 0:
            rmv = [pair for pair in tradedata.keys() \
                if pair not in get_pairs()]
            for k in rmv:
                del tradedata[k]
            [tradedata.update({pair:[]}) for pair in get_pairs() \
                if pair not in tradedata.keys()]

            openpairs = set([n['pair'] for n in db.trades.find({'status':'open'})])
            # Save non-duplicate trade prices for each pair being currently traded.
            for pair in openpairs:
                prices = list(set([n['close'] for n in items if n['pair'] == pair]))

                if len(prices) > 0:
                    tradedata[pair] += prices
                    if len(tradedata[pair]) > 10:
                        tradedata[pair] = tradedata[pair][-10:]
                    pprint(tradedata)
        print('{} open_candle queue items cleared.'.format(len(items)))
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
    db = app.db
    result = db.trades.insert_one(odict({
        'pair': candle['pair'],
        'quote_asset': db.assets.find_one({'symbol':candle['pair']})['quoteAsset'],
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
            'price': ss['book']['askPrice'],
            'volume': 1.0,
            'quote': TRADE_AMT_MAX,
            'fee': TRADE_AMT_MAX * (BINANCE_PCT_FEE/100)
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
    pct_fee = BINANCE_PCT_FEE
    bid = ss['book']['bidPrice']
    ask = ss['book']['askPrice']
    buy_vol = np.float64(record['orders'][0]['volume'])
    buy_quote = np.float64(record['orders'][0]['quote'])
    p1 = np.float64(record['orders'][0]['price'])

    pct_gain = pct_diff(p1, bid)
    quote = buy_quote * (1 - pct_fee/100)
    fee = (bid * buy_vol) * (pct_fee/100)
    pct_net_gain = net_earn = pct_gain - (pct_fee*2)
    pct_total_slippage = record['snapshots'][0]['book']['pctSlippage'] + ss['book']['pctSlippage']
    duration = now() - record['start_time']

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
                    'volume': 1.0,
                    'quote': buy_quote,
                    'fee': fee
                })
            },
            '$set': {
                'status': 'closed',
                'end_time': now(),
                'duration': int(duration.total_seconds()),
                'pct_gain': pct_gain.round(4),
                'pct_net_gain': pct_net_gain.round(4)
            }
        }
    )
    return record['_id']

#------------------------------------------------------------------------------
def snapshot(candle):
    """Gather state of trade--candle, indicators--each tick and save to DB.
    """
    global tradedata
    client = app.bot.client
    db = app.db

    book = odict(client.get_orderbook_ticker(symbol=candle['pair']))
    del book['symbol']
    [book.update({k:np.float64(v)}) for k,v in book.items()]
    book.update({
        'price': candle['close'],
        'pctSpread': round(pct_diff(book['bidPrice'], book['askPrice']),3),
        'pctSlippage': round(pct_diff(candle['close'], book['askPrice']),3)
    })

    buy_ratio = 0.0
    if float(candle['volume']) > 0:
        buy_ratio = (candle['buy_vol'] / candle['volume']).round(2)

    df = app.bot.dfc.loc[candle['pair'], strtofreq(candle['freqstr'])]
    dfh, phases = macd.histo_phases(df, candle['pair'], candle['freqstr'], 100)
    dfh['start'] = dfh.index
    dfh['duration'] = dfh['duration'].apply(lambda x: str(x.to_pytimedelta()))

    current = phases[-1].round(3)
    idx = current.index.to_pydatetime()
    current.index = [str(to_local(n.replace(tzinfo=pytz.utc))) for n in idx]

    trend = 0
    if candle['pair'] in tradedata.keys():
        pdiff = pd.Series(tradedata[candle['pair']]).diff()
        if len(pdiff) > 0:
            trend = pdiff.ewm(span=5).mean().iloc[-1]
            n_dec = Decimal(str(trend)).as_tuple().exponent
            trend = trend.round(n_dec)

    return odict({
        'pair': candle['pair'],
        'time': now(),
        'book': book,
        'candle': odict(candle),
        'indicators': odict({
            'buyratio': buy_ratio,
            'macd': current.values.tolist()[-1],
            'rsi': signals.rsi(df['close'], 14),
            'pricetrend': trend,
            'zscore': signals.zscore(df['close'], candle['close'], 21)
        }),
        'macd': odict({
            'histo': [{k:v} for k,v in current.to_dict().items()],
            'trend': current.diff().ewm(span=min(2, len(current))).mean().iloc[-1],
            'desc': current.describe().round(3).to_dict(),
            'history': dfh.to_dict('record')
        }),

    })
