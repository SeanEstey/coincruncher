import logging
from datetime import timedelta as delta
import pandas as pd
import numpy as np
from docs.conf import ema9
import app, app.bot
from app.common.timeutils import strtofreq
from . import candles

log = logging.getLogger('signals')

#-----------------------------------------------------------------------------
def rsi(df, span):
    """RSI indicator using EMA.
    @df: pandas time-series w/ close price as column.
    """
    diff = df.diff()
    gains = diff[diff > 0].ewm(span=span).mean()
    losses = diff[diff < 0].ewm(span=span).mean()
    rs = abs(gains.mean() / losses.mean())
    rsi = 100 - (100 / (1.0 + rs))
    return np.float64(rsi).round(2)

#-----------------------------------------------------------------------------
def ema_pct_change(candle, span=None):
    """Calculate percent change of candle 'close' price exponential moving
    average for preset time span.
    """
    _span = span if span else ema9[0]

    # Convert span to time range
    _range = {
        '1m': delta(minutes = span * 2),
        '5m': delta(minutes = 5 * span * 2),
        '1h': delta(hours = span * 2),
        '1d': delta(days = span * 2)
    }

    df = app.bot.dfc.loc[candle['pair'], strtofreq(candle['freq'])]
    sliced = df.loc[slice(
        candle['open_time'] - _range[candle['freq']],
        candle['open_time']
    )]['close'].copy()

    df_ema = sliced.ewm(span=span).mean().pct_change() * 100
    df_ema.index = [ str(x)[:-10] for x in df_ema.index.values ]
    return df_ema.round(3)

#-----------------------------------------------------------------------------
def z_score(candle, periods):
    """Generate Mean/STD/Z-Score for given candle properties.
    Attempts to correct distorted Mean values in bearish/bullish markets by
    adjusting length of historic period. Perf: ~20ms
    Returns: pd.DataFrame w/ [5 x 4] dimensions
    """
    df = app.bot.dfc.loc[candle['pair'], strtofreq(candle['freq'])]
    co, cf = candle['open_time'], candle['freq']

    if cf == '1m':
        end = co - delta(minutes=1)
        start = end - delta(minutes = periods)
    elif cf == '5m':
        end = co - delta(minutes=5)
        start = end - delta(minutes = 5 * periods)
    elif cf == '1h':
        end = co - delta(hours=1)
        start = end - delta(hours = periods)

    history = df.loc[slice(start, end)]

    # Smooth signal/noise ratio with EMA.
    ema = history.ewm(span=periods).mean()

    # Mean and SD
    stats = ema.describe()
    cols = ['close', 'volume', 'buy_ratio']

    # Calc Z-Scores
    data = [
        (candle['close'] - stats['close']['mean']) / stats['close']['std'],
        (candle['volume'] - stats['volume']['mean']) / stats['volume']['std'],
        (candle['buy_ratio'] - stats['buy_ratio']['mean']) / stats['buy_ratio']['std']
    ]

    return pd.Series(data, index=cols).astype('float64').round(8)

#------------------------------------------------------------------------------
def weighted_avg(df, col, weights):
    try:
        return (df[col] * weights).sum() / sum(weights)
    except Exception as e:
        log.error("Div/0 error. Returning unweighted mean.")
        return df[col].mean()



#-----------------------------------------------------------------------------
def vwap():
    """Writeme.
    buy volume * price / total volume
    """
    pass


