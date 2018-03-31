# strategy.py
import logging
from binance.client import Client
from app.common.utils import utc_datetime as now
import app.bnc
from app.bnc import signals

log = logging.getLogger('strategy')

zscore_range = {
    '1m': 1440,
    '5m': 288,
    '1h': 72,
    '1d': 7
}

#-------------------------------------------------------------------------------
def evaluate(side, candle, record=None):
    """Evaluate buy/sell decision based on available strategies.
    """
    dfc = app.bnc.dfc

    if side == 'buy':
        # A. Z-Score strat
        result = zthreshold(side, candle)
        if result:
            return result

        # B. Momentum strat
        result = momentum(side, candle)
        if result:
            return result
    elif side == 'sell':
        reason = record['buy']['criteria']['strategy']

        if reason == 'momentum':
            return momentum('sell', candle, record=record)
        elif reason == 'zthreshold':
            return zthreshold('sell', candle)

    # No trade executed.
    z = signals.z_score(candle, zscore_range[candle['freq']])
    ema = signals.ema_pct_change(candle)

    log.debug("{:<7} {:>+10.2f} z-p {:>+10.2f} z-v {:>10.2f} bv {:>+10.2f} m".format(
        candle['pair'], z.close, z.volume, candle['buy_ratio'], ema.iloc[-1]))
    return None

#------------------------------------------------------------------------------
def zthreshold(side, candle):
    """Evaluate z-score vs buy/sell thresholds.
    Buy: Z-Score below threshold
    Sell: Z-Score returning to mean
    """
    z = signals.z_score(candle, zscore_range[candle['freq']])

    snapshot = {
        'time': now(),
        'price': candle['close'],
        'volume': candle['volume'],
        'buy_ratio': candle['buy_ratio'],
        'z-score': z.to_dict()
    }

    if side == 'BUY':
        threshold = app.bnc.rules['Z-SCORE']['BUY_THRESH']
        if z.close > threshold:
            return None

        client = Client("","")
        ob = client.get_orderbook_ticker(symbol=candle['pair'])

        return {
            'strategy': 'zthreshold',
            'snapshot': {**snapshot, **{'ask_price':float(ob['askPrice'])}},
            'details': {
                'close:z-score < thresh': '{:+.2f} < {:+.2f}'.format(
                    z.close, threshold)
            }
        }
    elif side == 'SELL':
        threshold = app.bnc.rules['Z-SCORE']['SELL_THRESH']
        if z.close < threshold:
            return {'action':None, 'snapshot':snapshot}

        ema = signals.ema_pct_change(candle)
        snapshot['ema_pct_change'] = ema.iloc[-1]

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
def momentum(side, candle, record=None):
    """Evaluate positive momentum for buy/sell decision.
    Buy: on confirmation of price/volume.
    Sell: at peak price slope.
    FIXME: confirm high volume across multiple 1m candles, not just 1.
    """
    z = signals.z_score(candle, zscore_range[candle['freq']])
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

    if side == 'BUY':
        if (ema.tail(5) < 0).any():
            return False
        if z.volume < 2.0 or z.buy_ratio < 0.5:
            return False

        return {
            'strategy': 'momentum',
            'snapshot': {**snapshot, **{'ask_price':float(ob['askPrice'])}},
            'details': {
                'ema:slope:tail(5):min > thresh': '{:+.2f}% > 0'.format(m.tail(5).min()),
                'z-score:volume > thresh': '{:+.2f} > 0.5'.format(z.volume),
                'z-score:buy-ratio > thresh': '{:+.2f} > 0.5'.format(z.buy_ratio)
            }
        }
    elif side == 'SELL':
        if ema.iloc[-1] >= max([x['ema_pct_change'] for x in record['snapshots']]):
            return {'action':None, 'snapshot':snapshot}

        return {
            'action': 'sell',
            'snapshot': {**snapshot, **{'bid_price':float(ob['bidPrice'])}},
            'details': {
                'ema:slope <= thresh': '{:+.2f}% <= 0'.format(ema.iloc[-1]),
                'AND bid < buy': '{:.8f} < {:.8f}'.format(
                    ob['bidPrice'], record['snapshots'][0]['ask_price'])
            }
        }
