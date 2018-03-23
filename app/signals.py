import logging
from pymongo import UpdateOne, ReplaceOne
from datetime import datetime, timedelta
from pprint import pprint
import pandas as pd
import numpy as np
import app
from app import freqtostr, pertostr, strtofreq, strtoper, candles
from app.timer import Timer
from app.utils import to_local
from docs.config import Z_FACTORS, Z_DIMEN, Z_IDX_NAMES
from docs.trading import RULES
def siglog(msg): log.log(100, msg)
log = logging.getLogger('signals')

#-----------------------------------------------------------------------------
def z_score(dfc, candle, mkt_ma=None):
    """Generate Mean/STD/Z-Score for given candle properties.
    Attempts to correct distorted Mean values in bearish/bullish markets by
    adjusting length of historic period.
    Performance: ~20ms
    Returns:
        pd.DataFrame w/ [5 x 4] dimensions
    """
    t1 = Timer()

    # ********************************************************************
    # TODO: Optimize by trying to group period logically via new "settled"
    # price range, if possible.
    # ********************************************************************

    # If bull/bear market, shorten historic period range
    mkt_ma = mkt_ma if mkt_ma else 0
    shorten = 1.0
    if abs(mkt_ma) > 0.05 and abs(mkt_ma) < 0.1:
        shorten = 0.75
    elif abs(mkt_ma) >= 0.1 and abs(mkt_ma) < 0.15:
        shorten = 0.6
    elif abs(mkt_ma) > 0.15:
        shorten = 0.5

    if candle['FREQ'] == '1m':
        hist_end = candle['OPEN_TIME'] - timedelta(minutes=1)
        hist_start = hist_end - timedelta(hours = 2 * shorten)
    elif candle['FREQ'] == '5m':
        hist_end = candle['OPEN_TIME'] - timedelta(minutes=5)
        hist_start = hist_end - timedelta(hours = 2 * shorten)
    elif candle['FREQ'] == '1h':
        hist_end = candle['OPEN_TIME'] - timedelta(hours=1)
        hist_start = hist_end - timedelta(hours = 72 * shorten)

    history = dfc.loc[slice(hist_start, hist_end)]
    stats = history.describe()
    data = []

    # Insert mean/std/z-score etc for each column
    for x in Z_FACTORS:
        data.append([
            candle[x],
            stats[x]['mean'],
            stats[x]['std'],
            (candle[x] - stats[x]['mean']) / stats[x]['std']
        ])

    df = pd.DataFrame(np.array(data).transpose(),
        index=pd.Index(Z_DIMEN), columns=Z_FACTORS
    ).astype('float64').round(8)

    log.debug('Scores generated [{:,.0f}ms]'.format(t1))
    return df

#------------------------------------------------------------------------------
def adjust_support_level(freq_str, mkt_ma):
    """Correct for distorted Z-Score values in sudden bearish/bullish swings.
    A volatile bearish swing pushes Z-Scores downwards faster than the mean
    adjusts to represent the new "price level", creating innacurate deviation
    values for Z-Scores. Offset by temporarily lowering support threshold.
    """
    # Example: -0.1% MA and -2.0 support => -3.0 support
    if mkt_ma < 0:
        return RULES[freq_str]['Z-SCORE']['BUY_SUPT'] * (1 + (abs(mkt_ma) * 5))
    # Example: +0.1% MA and +2.0 support => +0.83 support
    else:
        return RULES[freq_str]['Z-SCORE']['BUY_SUPT'] * (1 - (mkt_ma * 1.75))

#------------------------------------------------------------------------------
def adjust_support_margin(freq_str, mkt_ma):
    """If bought as a bounce trade, allow some wiggle room for price to bottom
    out before bouncing up.
    ADDME: category field in holding to avoid this hack.
    ADDME: adjust value closer to 1.0 in bearish markets, closer to 1.01 in
    bull markets, to optimize risk/reward.
    """
    margin = RULES[freq_str]['Z-SCORE']['SELL_SUPT_MARG']

    if mkt_ma > 0:
        return margin
    else:
        return 1.0
