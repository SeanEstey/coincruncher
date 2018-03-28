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
from app.bnc import pct_diff, z_thresh, pairs, candles, markets, signals, printer

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
    # Update candles updated by websocket
    app.bnc.dfc = candles.merge_new(app.bnc.dfc, pairs, span=None)

    siglog('*'*80)
    duration = to_relative_str(now() - start)
    hdr = "Cycle #{} {:>%s}" % (80 - 7 - 1 - len(str(n_cycles)))
    siglog(hdr.format(n_cycles, duration))
    siglog('*'*80)
    siglog("{} trading pair(s):".format(len(pairs)))
    [siglog(x) for x in printer.agg_mkts().to_string().split('\n')]
    siglog('-'*80)
    printer.positions('open')
    siglog('-'*80)

    # Evaluate Sells
    active = list(get_db().trades.find({'status':'open', 'pair':{"$in":pairs}}))
    for trade in active:
        candle = candles.newest(trade['pair'], freq_str, df=app.bnc.dfc)
        trade_ids += [eval_sell(trade, candle)]

    # Evaluate Buys
    inactive = sorted(list(set(pairs) - set([n['pair'] for n in active])))
    for pair in inactive:
        candle = candles.newest(pair, freq_str, df=app.bnc.dfc)
        trade_ids += [eval_buy(candle)]

    printer.trades([n for n in trade_ids if n])
    siglog('-'*80)
    printer.positions('closed', start=start)

    n_cycles +=1

#-------------------------------------------------------------------------------
def eval_buy(candle):
    """New trade criteria.
    """
    dfc = app.bnc.dfc
    sig = signals.generate(candle)
    z = sig['z-score']

    # A. Z-Score below threshold
    if z.close < z_thresh:
        return buy(candle, decision={
            'category': 'z-score',
            'signals': sig,
            'details': {
                'close:z-score < thresh': '{:+.2f} < {:+.2f}'.format(
                    z.close, z_thresh)
            }
        })

    # B) Positive market & candle EMA (within Z-Score threshold)
    # 2/5 agg.mkt values > 0 (IN SEQUENCE) == .4 confidence
    # 3/5 agg.mkt values > 0 (IN SEQUENCE) == .6 confidence
    # 5/5 agg.mkt values > 0 (IN SEQUENCE) == .95 confidence
    # Positive aggregate trading pair price slope for 5 min and 60 min periods.
    span1 = 5
    span2 = 60
    agg_slope1 = markets.agg_pct_change(freq_str, span=span1, label='agg.slope').iloc[0][0]
    agg_slope2 = markets.agg_pct_change(freq_str, span=span2, label='agg.slope').iloc[0][0]

    if (sig['ema_slope'].tail(5) > 0).all():
        if agg_slope1 > 0 and agg_slope2 > 0 and z.volume > 0.5 and z.buy_ratio > 0.5:
            return buy(candle, decision={
                'category': 'slope',
                'signals': sig,
                'details': {
                    'ema:slope:tail(5):min > thresh': '{:+.2f}% > 0'.format(
                        sig['ema_slope'].tail(5).min()),
                    'agg:ema:slope > thresh': '{:+.4g}% (span={}) > 0'.format(agg_slope1, span1),
                    'agg:ema:slope > thresh': '{:+.4g}% (span={}) > 0'.format(agg_slope2, span2),
                    'volume:z-score > thresh': '{:+.2f} > 0.5'.format(z.volume),
                    'buy-ratio:z-score > thresh': '{:+.2f} > 0.5'.format(z.buy_ratio)
                }
            })

    # No buy executed.
    log.debug("{}{:<5}{:+.2f} Z-Score{:<5}{:+.2f} Slope".format(
        candle['pair'], '', z.close, '', sig['ema_slope'].iloc[-1]))
    return None

#------------------------------------------------------------------------------
def eval_sell(doc, candle):
    """Avoid losses, maximize profits.
    """
    global client
    sig = signals.generate(candle)
    doc = app.get_db().trades.find_one_and_update(
        {"_id":doc["_id"]},
        {"$push":{"monitor.ema_slope":sig['ema_slope'].iloc[-1]}},
        return_document = ReturnDocument.AFTER
    )
    reason = doc['buy']['decision']['category']

    # A. Predict price peak as we approach mean value.
    if reason == 'z-score':
        if sig['z-score'].close > -0.75:
            if sig['ema_slope'].iloc[-1] <= 0.10:
                return sell(doc, candle, decision={
                    'category': 'z-score',
                    'signals': sig,
                    'details': {
                        'close:z-score > thresh': '{:+.2f} > -0.75'.format(sig['z-score'].close),
                        'ema:slope < thresh': '{:+.2f}% <= 0.10'.format(sig['ema_slope'].iloc[-1])
                    }
                })
    # B. Sell at peak price slope
    elif reason == 'slope':
        ob = client.get_orderbook_ticker(symbol=candle['pair'])
        bid = float(ob['bidPrice'])

        if sig['ema_slope'].iloc[-1] < max(doc['monitor']['ema_slope']):
            return sell(doc, candle, orderbook=ob, decision={
                'category': 'slope',
                'signals': sig,
                'details': {
                    'ema:slope <= thresh': '{:+.2f}% <= 0'.format(sig['ema_slope'].iloc[-1]),
                    'AND bid < buy': '{:.8f} < {:.8f}'.format(bid, doc['buy']['order']['price'])
                }
            })
    return None

#------------------------------------------------------------------------------
def buy(candle, decision=None):
    """Create or update existing position for zscore above threshold value.
    """
    global client
    decision['signals']['z-score'] = decision['signals']['z-score'].to_dict()
    decision['signals']['ema_slope'] = sorted(decision['signals']['ema_slope'].to_dict().items())
    orderbook = client.get_orderbook_ticker(symbol=candle['pair'])
    order = {
        'exchange': 'Binance',
        'price': np.float64(orderbook['askPrice']),
        'volume': 1.0,  # FIXME
        'quote': BINANCE['TRADE_AMT'],
        'pct_fee': BINANCE['PCT_FEE'],
        'fee': BINANCE['TRADE_AMT'] * (BINANCE['PCT_FEE']/100),
    }

    return get_db().trades.insert_one({
        'pair': candle['pair'],
        'status': 'open',
        'start_time': now(),
        'buy': {
            'time': now(),
            'candle': candle,
            'decision': decision,
            'orderbook': orderbook,
            'order': order
        }
    }).inserted_id

#------------------------------------------------------------------------------
def sell(doc, candle, orderbook=None, decision=None):
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

    decision['signals']['z-score'] = decision['signals']['z-score'].to_dict()
    decision['signals']['ema_slope'] = sorted(decision['signals']['ema_slope'].to_dict().items())

    get_db().trades.update_one(
        {'_id': doc['_id']},
        {'$set': {
            'status': 'closed',
            'end_time': now(),
            'duration': int(duration.total_seconds()),
            'pct_pdiff': pct_pdiff.round(4),
            'pct_earn': pct_net.round(4),
            'net_earn': net_earn.round(4),
            'sell': {
                'time': now(),
                'candle': candle,
                'decision': decision,
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
        }}
    )
    return doc['_id']