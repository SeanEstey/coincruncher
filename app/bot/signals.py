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
    """RSI indicator for determining overbought/oversold prices.
    """
    diff = df.diff().ewm(span=span).mean()
    gains = diff[diff > 0]
    losses = diff[diff < 0]
    rs = abs(gains.mean() / losses.mean())
    rsi = 100 - (100 / (1.0 + rs))
    return int(rsi)

#-----------------------------------------------------------------------------
def z_score(df, columns, candle, span):
    """Calculate the number of standard deviations from the mean for each of the
    specified candle properties, given the historic values for each.
    """
    ema = df.ewm(span=span).mean()
    desc = ema.describe()
    z_values = [((candle[n] - desc[n]['mean']) / desc[n]['std']) for n in columns]
    return pd.Series(z_values, index=columns).astype('float64')

#------------------------------------------------------------------------------
def weighted_avg(values, weights):
    """Apply weight function to points in given series. Weights must be a
    series or numpy array of identical length.
    """
    try:
        return (values * weights).sum() / weights.sum()
    except Exception as e:
        log.error("Div/0 error. Returning unweighted mean.")
        return df[col].mean()

#-------------------------------------------------------------------------------
def support_resistance(df):
    """
    Algorithm

    Break timeseries into segments of size N (Say, N = 5)
    Identify minimum values of each segment, you will have an array of minimum
    values from all segments = :arrayOfMin
    Find minimum of (:arrayOfMin) = :minValue
    See if any of the remaining values fall within range (X% of :minValue)
    (Say, X = 1.3%)
    Make a separate array (:supportArr)
    add values within range & remove these values from :arrayOfMin
    also add :minValue from step 3
    Calculating support (or resistance)

    Take a mean of this array = support_level
    If support is tested many times, then it is considered strong.
    strength_of_support = supportArr.length
    level_type (SUPPORT|RESISTANCE) = Now, if current price is below support
    then support changes role and becomes resistance.
    Repeat steps 3 to 7 until :arrayOfMin is empty

    You will have all support/resistance values with a strength. Now smoothen
    these values, if any support levels are too close then eliminate one of
    them. hese support/resistance were calculated considering support levels
    search. You need perform steps 2 to 9 considering resistance levels search.

    Notes:
    Adjust the values of N & X to get more accurate results.
    Example, for less volatile stocks or equity indexes use (N = 10, X = 1.2%)
    For high volatile stocks use (N = 22, X = 1.5%)
    For resistance, the procedure is exactly opposite (use maximum function
    instead of minimum). This algorithm was purposely kept simple to avoid
    complexity, it can be improved to give better results.
    """
    pass

#-----------------------------------------------------------------------------
def vwap():
    """Writeme.
    buy volume * price / total volume
    """
    pass
