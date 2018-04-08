# strategy.py
from pprint import pprint
import logging
from binance.client import Client
from collections import OrderedDict as odict
import numpy as np
from app.common.utils import colors, utc_datetime as now
import app.bot
from app.bot import signals, printer
from docs.conf import strategies as strats
from app import strtofreq, freqtostr
def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)
log = logging.getLogger('strategy')

#-------------------------------------------------------------------------------
def update(candle, record):
    return _strategies[record['strategy']](candle, record=record)

#-------------------------------------------------------------------------------
def evaluate(candle):
    from docs.conf import max_positions
    n_active = app.get_db().trades.find({'status':'open'}).count()
    if n_active >= max_positions:
        return [{'action':'SKIP'}]

    # Execute any viable strategies
    results=[]
    for k,v in _strategies.items():
        results.append(v(candle))
    return results

#------------------------------------------------------------------------------
def my_macd(candle, record=None):
    """Return one of 3 actions: BUY, SELL, HODL, SKIP
    TODO: implement dollar cost averaging into position as MACD decreases.
    dollar cost average out as MACd increases.
    """
    conf = strats['macd']
    ss = snapshot(candle)
    ss['strategy'] = 'macd'
    macd, histo, desc = ss['macd']['value'], ss['macd']['histo'], ss['macd']['desc']

    # BUY options.
    if record is None:
        ss['details'] += \
            'Volume Z-Score is {0:+.1f}.\n'.format(ss['volume']['z-score'])

        if candle['freq'] not in conf['freq'] or \
            len(histo) == 1 or \
            macd >= 0 or \
            macd < list(histo.values())[-2] or \
            macd == desc['min'] or \
            ss['volume']['z-score'] < conf['min_volume_zscore'] or \
            ss['buyRatio']['value'] < conf['min_buy_ratio']:
            return {'action':'SKIP', 'snapshot':ss}
        else:
            return {'action':'BUY', 'snapshot':ss}
    # SELL/HODL options
    elif record:
        stop_loss = 0.005
        if candle['close'] < record['orders'][0]['candle']['close'] * (1 - stop_loss):
            ss['details'] += \
                'Stop loss executed at {0:.2f}% below buy price.\n'\
                'Bought at {1:g}, now at {2:g}.\n'\
                .format(
                    stop_loss * 100,
                    record['orders'][0]['price'],
                    candle['close'])
            return {'action': 'SELL', 'snapshot': ss}

        if macd > 0 and macd < desc['max']:
            # Optimal selling point.
            return {'action': 'SELL', 'snapshot': ss}
        else:
            return {'action':'HOD', 'snapshot':ss}

#------------------------------------------------------------------------------
def _zscore(candle, record=None):
    """Evaluate z-score vs buy/sell thresholds.
    Buy: Z-Score below threshold
    Sell: Z-Score returning to mean
    """
    if candle['freq'] != '1m':
        return None

    conf = strats['z-score']
    periods = conf['periods']
    z = signals.z_score(candle, periods)
    ema = signals.ema_pct_change(candle, conf['ema'])

    snapshot = {
        'time': now(),
        'price': candle['close'],
        'volume': candle['volume'],
        'buy_ratio': candle['buy_ratio'],
        'z-score': z.to_dict(),
        'ema_pct_change': ema.iloc[-1]
    }

    if record is None:
        threshold = conf['buy_thresh']
        if z.close > threshold:
            print('z.close < threshold')
            return None

        client = Client("","")
        ob = client.get_orderbook_ticker(symbol=candle['pair'])

        return {
            'strategy': 'z-score',
            'snapshot': {**snapshot, **{'ask_price':float(ob['askPrice'])}},
            'details': {
                'close:z-score < thresh': '{:+.2f} < {:+.2f}'.format(
                    z.close, threshold)
            }
        }
    elif record:
        threshold = conf['sell_thresh']
        if z.close < threshold:
            return {'action':None, 'snapshot':snapshot}

        if ema.iloc[-1] > 0.10:
            return {'action':None, 'snapshot':snapshot}

        client = Client("","")
        ob = client.get_orderbook_ticker(symbol=candle['pair'])

        return {
            'action': 'sell',
            'snapshot': {**snapshot, **{'bid_price':float(ob['bidPrice'])}},
            'details': {
                'close:z-score > thresh': '{:+.2f} > -0.75'.format(z.close),
                'ema:slope < thresh': '{:+.2f}% <= 0.10'.format(ema.iloc[-1])
            }
        }

#------------------------------------------------------------------------------
def my_momentum(candle, record=None):
    """Evaluate positive momentum for buy/sell decision.
    Buy: on confirmation of price/volume.
    Sell: at peak price slope.
    FIXME: confirm high volume across multiple 1m candles, not just 1.
    """
    if candle['freq'] != '5m':
        return None

    conf = strats['ema']
    periods = conf['span']
    z = signals.z_score(candle, periods)
    ema = signals.ema_pct_change(candle)

    client = Client("","")
    ob = client.get_orderbook_ticker(symbol=candle['pair'])

    snapshot = {
        'time': now(),
        'candle_price': candle['close'],
        'volume': candle['volume'],
        'buy_ratio': candle['buy_ratio'],
        'z-score': z.to_dict(),
        'ema_pct_change': ema.iloc[-1]
    }

    if record is None:
        if (ema.tail(5) < 0).any():
            print('ema.tail(5).any() < 0')
            return False
        if z.volume < 2.0 or z.buy_ratio < 0.5:
            print('z.volume < 2.0 or z.buy_ratio < 0.5')
            return False

        return {
            'strategy': 'momentum',
            'snapshot': {**snapshot, **{'ask_price':float(ob['askPrice'])}},
            'details': {
                'ema:slope:tail(5):min > thresh': '{:+.2f}% > 0'.format(ema.tail(5).min()),
                'z-score:volume > thresh': '{:+.2f} > 0.5'.format(z.volume),
                'z-score:buy-ratio > thresh': '{:+.2f} > 0.5'.format(z.buy_ratio)
            }
        }
    elif record:
        if ema.iloc[-1] >= max([x['ema_pct_change'] for x in record['snapshots']]):
            return {'action':None, 'snapshot':snapshot}

        return {
            'action': 'sell',
            'snapshot': {**snapshot, **{'bid_price':float(ob['bidPrice'])}},
            'details': {
                'ema:slope <= thresh': '{:+.2f}% <= 0'.format(ema.iloc[-1]),
                'AND bid < buy': '{:} < {:.8f}'.format(
                    ob['bidPrice'], record['snapshots'][0]['ask_price'])
            }
        }
