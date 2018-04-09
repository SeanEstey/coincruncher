# strategy.py
import logging
from docs.conf import stop_loss

log = logging.getLogger('strategy')

#------------------------------------------------------------------------------
def my_macd(candle, ss, conf, record=None):
    """Return one of 4 actions: BUY, SELL, HODL, SKIP
    TODO: implement dollar cost averaging into position as MACD decreases.
    dollar cost average out as MACd increases.
    """
    macd = ss['macd']['value']
    desc = ss['macd']['desc']

    # Buy/Skip
    if record is None:
        if macd <= 0:
            return {'action':'SKIP'}
        else:
            return {'action':'BUY'}

    last = record['orders'][0]['candle']

    # Manage existing position
    if candle['close'] < last['close']*(1-stop_loss):
        return {
            'action':'SELL',
            'details':'Stop loss. Price fell > {0:.2f}%.'\
                .format(stop_loss*100)
        }

    if macd < 0:
        print("SELLING %s" % candle['pair'])
        print(ss['macd'])
        return {'action':'SELL', 'details':'Histogram in (-) phase.'}

    return {'action':'HODL'}

#------------------------------------------------------------------------------
def my_zscore(candle, ss, conf=None, record=None):
    """Evaluate z-score vs buy/sell thresholds.
    Buy: Z-Score below threshold
    Sell: Z-Score returning to mean
    """
    if candle['freq'] != '1m':
        return None

    from docs.conf import strategies as strats
    conf = strats['z-score']
    periods = conf['periods']

    if record is None:
        threshold = conf['buy_thresh']
        if z.close > threshold:
            print('z.close < threshold')
            return None

        return {
            'strategy': 'z-score',
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

        return {
            'action': 'sell',
            'details': {
                'close:z-score > thresh': '{:+.2f} > -0.75'.format(z.close),
                'ema:slope < thresh': '{:+.2f}% <= 0.10'.format(ema.iloc[-1])
            }
        }
