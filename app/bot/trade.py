# app.bot.trade
import time
import logging
import pytz
import numpy as np
import pandas as pd
from pprint import pprint
from collections import OrderedDict as odict
from docs.conf import *
from docs.botconf import *
import app, app.bot
from app.bot import get_pairs, candles, macd, reports, signals
from app.common.timeutils import strtofreq
from app.common.utils import pct_diff, utc_datetime as now
from app.common.timer import Timer

def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)
log = logging.getLogger('trade')
dfW = pd.DataFrame()
start = now()

#---------------------------------------------------------------------------
def run(e_pairs):
    """Main trading loop thread. Consumes candle data from queue and
    manages/executes trades.
    """
    from .candles import columns
    from main import q
    global dfW
    n, ent_ids, ex_ids = 0, [], []
    db = app.get_db()
    dfc = app.bot.dfc
    print("start trade loop")
    tmr = Timer(name='positions', expire='every 1 clock min utc')

    while True:
        while q.empty() == False:
            c = q.get()
            ss = snapshot(c)
            query = \
                {'pair':c['pair'], 'freqstr':c['freqstr'], 'status':'open'}

            if c['closed']:
                candles.append_df(c)
                db.trades.update_many(query, {'$push':{'snapshots':ss}})

                # Clear all partial candle data
                #dfW = dfW.drop([(c['pair'], strtofreq(c['freqstr']))])
            else:
                freq = strtofreq(c['freqstr'])
                dfc.ix[(c['pair'],freq)].iloc[-1] = [c[n] for n in columns[3:]]

            # Eval position entries/exits
            for trade in db.trades.find(query):
                ex_ids += eval_exit(trade, c, ss)
            if c['closed']:
                ent_ids += eval_entry(c, ss)

            if len(ent_ids) > 0:
                reports.new_trades(ent_ids)
            if len(ent_ids) + len(ex_ids) > 0:
                reports.positions()
                reports.earnings()

            if tmr.remain(quiet=True) == 0:
                reports.positions()
                tmr.reset()
            n+=1
            # End Inner While
        if n > 0:
            print('{} queue items emptied.'.format(n))

        n, ent_ids, ex_ids = 0, [], []
        time.sleep(0.5)

#------------------------------------------------------------------------------
def eval_entry(c, ss):
    db = app.get_db()
    ids = []
    for algo in TRADE_ALGOS:
        if db.trades.find_one(
            {'freqstr':c['freqstr'], 'algo':algo['name'], 'status':'open'}):
            continue

        # Test conditions eval to True
        if all([fn(ss['indicators']) for fn in algo['entry']['conditions']]):
            ids.append(
                buy(c, algo, ss))
    return ids

#------------------------------------------------------------------------------
def eval_exit(trade, c, ss):
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
            sell(trade, c, ss, details="Failure conditions met"))

    return ids

#------------------------------------------------------------------------------
def buy(c, algo, ss):
    """Open new position, insert record to DB.
    """
    db = app.db
    result = db.trades.insert_one(odict({
        'pair': c['pair'],
        'quote_asset': db.assets.find_one({'symbol':c['pair']})['quoteAsset'],
        'freqstr': c['freqstr'],
        'status': 'open',
        'start_time': now(),
        'algo': algo['name'],
        'stoploss': algo['stoploss'],
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

    print("BUY {} ({})".format(c['pair'], algo['name']))
    return result.inserted_id

#------------------------------------------------------------------------------
def sell(record, c, ss, details=None):
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
        .format(c['pair'], record['algo'], details))

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
def snapshot(c):
    """Gather state of trade--candle, indicators--each tick and save to DB.
    """
    global dfW
    pair, freqstr = c['pair'], c['freqstr']
    book = odict(app.bot.client.get_orderbook_ticker(symbol=pair))
    del book['symbol']
    [book.update({k:np.float64(v)}) for k,v in book.items()]
    buyratio = (c['buy_vol']/c['volume']) if c['volume'] > 0 else 0.0

    # MACD Indicators
    df = app.bot.dfc.loc[pair, strtofreq(freqstr)]
    try:
        dfmacd, phases = macd.histo_phases(df, pair, freqstr, 100, to_bson=True)
    except Exception as e:
        print(str(e))
        log.exception(str(e))

    dfm_dict = dfmacd.iloc[-1].to_dict()
    dfm_dict['bars'] = int(dfm_dict['bars'])
    phase = phases[-1]
    amp_slope = phase.diff().ewm(span=len(phase)).mean().iloc[-1]

    wick_slope = np.nan
    if c['closed']:
        # Find price EMA WITHIN the wick (i.e. each trade). Very
        # small movements.
        #prices = dfW.loc[c['pair'], c['freqstr']]['close']
        #wick_slope = prices.diff().ewm(span=len(prices)).mean().iloc[-1]
        # FIXME
        wick_slope = 0.0

    return {
        'pair': pair,
        'time': now(),
        'book': book,
        'candle': c,
        'indicators': {
            'buyRatio': round(buyratio, 2),
            'rsi': signals.rsi(df['close'], 14),
            'wickSlope': wick_slope,
            'zscore': signals.zscore(df['close'], c['close'], 21),
            'macd': {
                **dfm_dict,
                **{'ampSlope':amp_slope,
                  'value':phases[-1].tolist()[-1]
                  }
            }
        }
    }
