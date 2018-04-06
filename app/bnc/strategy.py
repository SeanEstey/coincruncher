# strategy.py
from pprint import pprint
import logging
from binance.client import Client
from collections import OrderedDict as odict
import numpy as np
from app.common.utils import colors, utc_datetime as now
import app.bnc
from app.bnc import signals, printer
from docs.rules import RULES as rules
from app import strtofreq, freqtostr
def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)
log = logging.getLogger('strategy')

#-------------------------------------------------------------------------------
def update(candle, record):
    return _strategies[record['strategy']](candle, record=record)

#-------------------------------------------------------------------------------
def evaluate(candle):
    from docs.rules import MAX_POSITIONS
    n_active = app.get_db().trades.find({'status':'open'}).count()
    if n_active >= MAX_POSITIONS:
        return [{'action':'SKIP'}]

    # Execute any viable strategies
    results=[]
    for k,v in _strategies.items():
        results.append(v(candle))
    return results

#------------------------------------------------------------------------------
def snapshot(candle):
    z = signals.z_score(candle, rules['z-score']['periods']).to_dict()
    client = Client("","")
    ob = client.get_orderbook_ticker(symbol=candle['pair'])

    df = app.bnc.dfc.loc[candle['pair'], strtofreq[candle['freq']]]
    macd = signals.macd(
        df,
        rules['macd']['short_period'],
        rules['macd']['long_period']
    )

    # Isolate histogram group
    last = np.float64(macd.tail(1)['macd_diff'])
    if last < 0:
        marker = macd[macd['macd_diff'] > 0].iloc[-1]
    else:
        marker = macd[macd['macd_diff'] < 0].iloc[-1]
    histo = macd.loc[slice(marker.name, macd.iloc[-1].name)].iloc[1:]['macd_diff']
    # Convert datetime index to str for mongodb storage.
    histo.index = [ str(n)[:-10] for n in histo.index.values ]

    return odict({
        'time': now(),
        'strategy': None,
        'details': None,
        'price': odict({
            'close': candle['close'],
            'z-score': round(z['close'], 2),
            'emaDiff': signals.ema_pct_change(
                candle, rules['ema']['span']).iloc[-1],
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
            'histo': histo.round(10).to_dict(odict),
            'desc': histo.describe().round(10).to_dict()
        })
    })

#------------------------------------------------------------------------------
def _macd(candle, record=None):
    """
    Return one of 3 actions: BUY, SELL, HODL, SKIP
    TODO: implement dollar cost averaging into position as MACD decreases.
    dollar cost average out as MACd increases.
    """
    ss = snapshot(candle)
    ss['strategy'] = 'macd'

    # Buy after the bottom peak on macd histogram as the price slope
    # begins rising again.
    if record is None:
        if candle['freq'] not in rules['macd']['freq'] or \
            ss['volume']['z-score'] < 0 or \
            len(ss['macd']['histo']) < 1 or \
            ss['macd']['value'] >= 0 or \
            abs(ss['macd']['value']) >= abs(ss['macd']['desc']['mean']):
        # END
            ss['details'] = \
                'Macd: {0:+g}, Histo.mean: {1:+g}, '\
                'Vol.Z-score: {2:+.1f}.'\
                .format(
                    ss['macd']['value'],
                    ss['macd']['desc']['mean'],
                    ss['volume']['z-score'])
            return {'action':'SKIP', 'snapshot':ss}
        else:
            ss['details'] = \
                'MACD below zero, trending UPWARD. '\
                'Bottom was {0:+g}, mean is {1:+g}, now at {2:+g}. '\
                'Volume Z-Score above zero at {3:+.1f}.'\
                .format(
                    float(ss['macd']['desc']['min']),
                    float(ss['macd']['desc']['mean']),
                    float(ss['macd']['value']),
                    float(ss['volume']['z-score']))
            return {'action':'BUY', 'snapshot':ss}
    elif record:
        # percent max loss
        stop_loss = 0.005
        if candle['close'] < record['orders'][0]['candle']['close'] * (1 - stop_loss):
            ss['details'] = \
                'Stop loss executed at {0:.2f}% below buy price. '\
                'Bought at {1:g}, now at {2:g}.'\
                .format(
                    stop_loss * 100,
                    record['orders'][0]['price'],
                    candle['close'])
            return {'action': 'SELL', 'snapshot': ss}

        # Don't sell if histogram hasn't peaked
        if ss['macd']['value'] < 0 or \
            abs(ss['macd']['value']) >= abs(ss['macd']['desc']['max']):

            ss['details'] = \
                'MACD above zero, trending UPWARD. '\
                'Now at {0:+g}, mean is {1:+g}.'\
                .format(
                    float(ss['macd']['value']),
                    float(ss['macd']['desc']['mean']))
            return {'action':'HODL', 'snapshot':ss}
        else:
            ss['details'] = \
                'MACD above zero, trending DOWNWARD. '\
                'Peaked at {0:+g}, now {1:+g}.'\
                .format(
                    float(ss['macd']['desc']['max']),
                    float(ss['macd']['value']))
            return {'action': 'SELL', 'snapshot': ss}

#------------------------------------------------------------------------------
def _zscore(candle, record=None):
    """Evaluate z-score vs buy/sell thresholds.
    Buy: Z-Score below threshold
    Sell: Z-Score returning to mean
    """
    if candle['freq'] != '1m':
        return None

    periods = rules['z-score']['periods']
    z = signals.z_score(candle, periods)
    ema = signals.ema_pct_change(candle, rules['ema']['span'])

    snapshot = {
        'time': now(),
        'price': candle['close'],
        'volume': candle['volume'],
        'buy_ratio': candle['buy_ratio'],
        'z-score': z.to_dict(),
        'ema_pct_change': ema.iloc[-1]
    }

    if record is None:
        threshold = rules['z-score']['buy_thresh']
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
        threshold = rules['z-score']['sell_thresh']
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
def _momentum(candle, record=None):
    """Evaluate positive momentum for buy/sell decision.
    Buy: on confirmation of price/volume.
    Sell: at peak price slope.
    FIXME: confirm high volume across multiple 1m candles, not just 1.
    """
    if candle['freq'] != '5m':
        return None

    periods = rules['z-score']['periods']
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

_strategies = {
    'macd': _macd,
    #'z-score': _zscore,
    #'momentum': _momentum
}
