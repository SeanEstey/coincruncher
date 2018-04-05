# strategy.py
import logging
from binance.client import Client
import numpy as np
from app.common.utils import colors, utc_datetime as now
import app.bnc
from app.bnc import signals
from docs.rules import RULES as rules
from app import strtofreq, freqtostr
def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)
log = logging.getLogger('strategy')

#-------------------------------------------------------------------------------
def update(candle, record):
    return _strategies[record['buy']['strategy']](candle, record=record)

#-------------------------------------------------------------------------------
def evaluate(candle):
    from docs.rules import MAX_POSITIONS
    n_active = app.get_db().trades.find({'status':'open'}).count()
    if n_active >= MAX_POSITIONS:
        return []

    # Execute any viable strategies
    results=[]
    for k,v in _strategies.items():
        results.append(v(candle))
    return [ x for x in results if x is not None ]

#------------------------------------------------------------------------------
def snapshot(candle):
    # FIXME: add proper rule check for correct freq
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
    marker = macd[macd['macd_diff'] > 0].iloc[-1] if last < 0 else macd[macd['macd_diff'] < 0].iloc[-1]
    histo = macd.loc[slice(marker.name, macd.iloc[-1].name)].iloc[1:]['macd_diff']

    return {
        'time': now(),
        'price': candle['close'],
        'ask_price': float(ob['askPrice']),
        'bid_price': float(ob['bidPrice']),
        'volume': candle['volume'],
        'buy_ratio': candle['buy_ratio'],
        'z_close': round(z['close'], 2),
        'z_volume': round(z['volume'], 2),
        'z_buy_ratio': round(z['buy_ratio'], 2),
        'macd_diff': last.round(5),
        'macd_histogram': histo.describe().round(5).to_dict(),
        'ema_pct_change': signals.ema_pct_change(
            candle,
            rules['ema']['span']
        ).iloc[-1]
    }

#------------------------------------------------------------------------------
def _macd(candle, record=None):
    """
    TODO: implement dollar cost averaging into position as MACD decreases.
    dollar cost average out as MACd increases.
    """
    # Analyze current phase histogram
    df = signals.macd(app.bnc.dfc.loc[candle['pair'], strtofreq[candle['freq']]],
        rules['macd']['short_period'], rules['macd']['long_period'])
    last = np.float64(df.tail(1)['macd_diff'])
    # Isolate histogram group
    marker = df[df['macd_diff'] > 0].iloc[-1] if last < 0 else df[df['macd_diff'] < 0].iloc[-1]
    hist = df.loc[slice(marker.name, df.iloc[-1].name)].iloc[1:]['macd_diff']

    if record is None:
        # Buy on peak negative histogram value.
        if candle['freq'] not in rules['macd']['freq'] or \
           len(hist) < 1 or \
           last >= 0 or \
           abs(last) >= abs(hist.describe()['mean']):
            return
        else:
            ss = snapshot(candle)
            return {
                'strategy': 'macd',
                'snapshot': ss,
                'details': {
                    'macd_diff > 0': '{:+.2f} > 0'.format(float(ss['macd_diff']))
                }
            }
    elif record:
        ss = snapshot(candle)
        # Don't sell if histogram hasn't peaked
        if last < 0 or \
           abs(last) >= abs(hist.describe()['mean']):
            return {'action':None, 'snapshot':ss}
        else:
            return {
                'action': 'sell',
                'snapshot': ss,
                'details': {
                    'macd_diff < 0': '{:+.2f} < 0'.format(float(ss['macd_diff']))
                }
            }

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
