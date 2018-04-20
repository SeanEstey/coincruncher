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
dfW = pd.DataFrame()
start = now()

#---------------------------------------------------------------------------
def run(e_pairs):
    """Main trading loop thread.
    """
    from main import q
    global dfW
    db = app.get_db()

    while True:
        enterids, exitids = [], []
        qsize = q.qsize()

        while not q.empty():
            c = q.get()
            ss = snapshot(c)
            query = \
                {'pair':c['pair'], 'freqstr':c['freqstr'], 'status':'open'}

            if not c['closed']:
                df = pd.DataFrame.from_dict([c], orient='columns')\
                    .set_index(['pair','freqstr','open_time'])
                dfW = dfW.append(df).drop_duplicates()
            else:
                # Save snapshot for closed candles only.
                db.trades.update_many(query, {'$push':{'snapshots':ss}})
                # Clear all partial candle data
                dfW = dfW.drop([(c['pair'], strtofreq(c['freqstr']))])

            # Eval position exits.
            for trade in db.trades.find(query):
                exitids += eval_exit(trade, c, ss)

            # New position entries.
            if c['closed']:
                enterids += eval_entry(c, ss)
        # End Inner While

        if len(enterids) > 0:
            reports.new_trades(enterids)

        reports.positions()

        if len(enterids) + len(exitds) > 0:
            reports.earnings()

        print('{} q items emptied.'.format(qsize))
        time.sleep(5)
    # End Outer While

#------------------------------------------------------------------------------
def eval_entry(candle, ss):
    db = app.get_db()
    ids = []
    for algo in TRADE_ALGOS:
        if db.trades.find_one(
            {'freqstr':candle['freqstr'], 'algo':algo['name'], 'status':'open'}):
            continue

        # Test conditions eval to True
        if all([fn(candle, ss['indicators']) for fn in algo['entry']['conditions']]):
            ids.append(
                buy(candle, algo, ss))
    return ids

#------------------------------------------------------------------------------
def eval_exit(trade, candle, ss):
    ids = []
    algo = [n for n in TRADE_ALGOS if n['name'] == trade['algo']][0]

    # Stop loss.
    diff = pct_diff(trade['snapshots'][0]['candle']['close'], c['close'])
    if diff < algo['stoploss']:
        sell(trade, c, ss, details='Stop Loss')

    # Target (success)
    if all([ fn(ss['indicators']) for fn in algo['target']['conditions'] ]):
        ids.append(
            sell(trade, c, ss, details="Target conditions met"))

    # Failure
    if all([ fn(ss['indicators']) for fn in algo['failure']['conditions'] ]):
        ids.append(
            sell(trade_, candle, ss, details="Failure conditions met"))

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
    global dfW
    c = candle
    pair, freqstr = c['pair'], c['freqstr']
    book = odict(app.bot.client.get_orderbook_ticker(symbol=pair))
    del book['symbol']
    [book.update({k:np.float64(v)}) for k,v in book.items()]
    buyratio = (c['buy_vol']/c['volume']) if c['volume'] > 0 else 0

    # MACD Indicators
    df = app.bot.dfc.loc[pair, strtofreq(freqstr)]
    dfmacd, phases = macd.histo_phases(df, pair, freqstr, 100, to_bson=True)
    phase = phases[-1]
    amp_slope = phase.diff().ewm(span=len(phase)).mean().iloc[-1]

    wick_slope = np.nan
    if c['closed']:
        # Find price EMA WITHIN the wick (i.e. each trade). Very
        # small movements.
        prices = dfW.loc[c['pair'], c['freqstr']]['close']
        wick_slope = prices.diff().ewm(span=len(prices)).mean().iloc[-1]

    return odict({
        'pair': pair,
        'time': now(),
        'book': book,
        'candle': odict(c),
        'indicators': odict({
            'buyRatio': buyratio.round(2),
            'rsi': signals.rsi(df['close'], 14),
            'wickSlope': wick_slope,
            'zscore': signals.zscore(df['close'], c['close'], 21),
            'macd': odict({
                **dfmacd.tail(1).to_dict()),
                **{'ampSlope':amp_slope,
                  'value':phase.values.tolist()[-1]}
            })
        })
    })
