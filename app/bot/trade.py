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
cache = {}
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
            app.bot.dfc = candles.bulk_load(get_pairs(), TRADEFREQS,
                dfm=app.bot.dfc)

            n_cycles += 1
            tradelog('*'*TRADELOG_WIDTH)
            duration = to_relative_str(now() - start)
            hdr = "Cycle #{} {:>%s}" % (31 - len(str(n_cycles)))
            tradelog(hdr.format(n_cycles, duration))
            tradelog('-'*TRADELOG_WIDTH)

        while q_closed.empty() == False:
            candle = q_closed.get()
            ss = snapshot(candle)
            #db.ss.insert_one(ss)
            exited = eval_exit(candle, ss)
            entered = eval_entry(candle, ss)
            n+=1

        if n > 0:
            print('{} q_closed items emptied.'.format(n))
            if entered:
                reports.new_trades(entered)
                tradelog('-'*TRADELOG_WIDTH)

            reports.positions()
            tradelog('-'*TRADELOG_WIDTH)

            if entered or exited:
                reports.earnings()

        time.sleep(10)

#------------------------------------------------------------------------------
def part_klines(e_pairs):
    """consume items from open candle queue. Track trade prices for open
    # positions and invoke stop losses when necessary.
    """
    from main import q_open
    global cache
    db = app.get_db()
    tmr = Timer(name='open_trade_eval', expire='every 1 clock min utc')

    while True:
        if q_open.empty() == False:
            items = []
            while q_open.empty() == False:
                candle = q_open.get()
                items.append(candle)

                # Stop loss
                query = {'pair':candle['pair'], 'freqstr':candle['freqstr'], 'status':'open'}
                for trade in db.trades.find(query):
                    diff = pct_diff(trade['snapshots'][0]['candle']['close'], candle['close'])
                    if diff < trade['stoploss']:
                        sell(trade, candle, snapshot(candle), details='Stop Loss')

            savecache(items)
            print('{} q_open items empties.'.format(len(items)))

        # Eval target/failure conditions via unclosed candles.
        if tmr.remain(quiet=True) == 0:
            for trade in db.trades.find({'status':'open'}):
                c = [n for n in cache[trade['pair']] if n['freqstr'] == trade['freqstr']]
                if len(c) > 0:
                    ss = snapshot(c[-1], last_ss=trade['snapshots'][0])
                    eval_exit(c[-1], ss)

            tmr.reset(quiet=True)
            reports.positions()
            tradelog('-'*TRADELOG_WIDTH)

        time.sleep(10)

#------------------------------------------------------------------------------
def savecache(candledata):
    """Update global trade price tracker list.
    """
    global cache
    db = app.get_db()

    # Update cache if changes to enabled pairs
    rmv = [pair for pair in cache.keys() if pair not in get_pairs()]
    for k in rmv:
        del cache[k]

    [cache.update({pair:[]}) for pair in get_pairs() \
        if pair not in cache.keys()]

    # Save non-duplicate trade prices for each pair being currently traded.
    openpairs = set([n['pair'] for n in db.trades.find({'status':'open'})])
    for pair in openpairs:
        _candles = [n for n in candledata if n['pair'] == pair]

        if len(_candles) > 0:
            cache[pair] += _candles

            if len(cache[pair]) > CACHE_SIZE:
                cache[pair] = cache[pair][-CACHE_SIZE:]

#------------------------------------------------------------------------------
def eval_entry(candle, ss):
    db = app.get_db()
    ids = []
    for algo in TRADE_ALGOS:
        if db.trades.find_one(
            {'freqstr':candle['freqstr'], 'algo':algo['name'], 'status':'open'}):
            continue

        # Test all filters/conditions eval to True
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

        if all([fn(candle, ss, trade) for fn in algo['target']['conditions']]):
            ids.append(sell(trade, candle, ss,
                details="Target conditions met"))
        elif all([fn(candle, ss, trade) for fn in algo['failure']['conditions']]):
            ids.append(sell(trade, candle, ss,
                details="Failure conditions met"))
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
def snapshot(candle, last_ss=None):
    """Gather state of trade--candle, indicators--each tick and save to DB.
    """
    global cache
    client = app.bot.client
    db = app.db
    buyratio = 0
    _macd, _indicators, _interim = {}, {}, {}
    pair, freqstr = candle['pair'], candle['freqstr']

    book = odict(client.get_orderbook_ticker(symbol=pair))
    del book['symbol']
    [book.update({k:np.float64(v)}) for k,v in book.items()]
    book.update({
        'price': candle['close'],
        'pctSpread': round(pct_diff(book['bidPrice'], book['askPrice']),3),
        'pctSlippage': round(pct_diff(candle['close'], book['askPrice']),3)
    })

    if float(candle['volume']) > 0:
        buyratio = (candle['buy_vol'] / candle['volume']).round(2)

    # Generate interim price movement (since opening position) from unclosed (cached)
    # candle data.
    if candle['ended'] == False and last_ss:
        _macd = last_ss['macd']
        _indicators = last_ss['indicators']
        prices = [n['close'] for n in cache[pair] if n['freqstr'] == freqstr]
        _interim['n_prices'] = len(prices)
        pdiff = pd.Series(prices).diff()

        if len(pdiff) > 0:
            _interim['pricetrend'] = pdiff.ewm(span=len(pdiff)).mean().iloc[-1]
        else:
            _interim['pricetrend'] = np.nan
    else:
        df = app.bot.dfc.loc[pair, strtofreq(freqstr)]
        dfmacd, phases = macd.histo_phases(df, pair, freqstr, 100, to_bson=True)
        _macd = odict({
            'histo': [{k:v} for k,v in phases[-1].to_dict().items()],
            'trend': phases[-1].diff().ewm(span=min(2, len(phases[-1]))).mean().iloc[-1],
            'desc': phases[-1].describe().round(3).to_dict(),
            'history': dfmacd.to_dict('record')
        })
        _indicators = odict({
            'buyratio': buyratio,
            'macd': phases[-1].values.tolist()[-1],
            'rsi': signals.rsi(df['close'], 14),
            'zscore': signals.zscore(df['close'], candle['close'], 21)
        })
        _interim['pricetrend'] = np.nan

    return odict({
        'pair': pair,
        'time': now(),
        'book': book,
        'candle': odict(candle),
        'indicators': _indicators,
        'interim': _interim,
        'macd': _macd
    })
