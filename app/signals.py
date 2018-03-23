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
def generate(dfc, candle, mkt_ma=None):
    """Generate Z-Scores and X-score.
    Performance: ~20ms
    """
    t1 = Timer()

    mkt_ma = mkt_ma if mkt_ma else 0
    shorten = 1.0

    # If bull/bear market, shorten historic period range
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
def xscore(z_scores, freq_str):
    """Apply weightings to Z-Scores.
    """
    weights = RULES[freq_str]['X-SCORE']['WEIGHTS']
    return (z_scores * weights).sum() / sum(weights)

#------------------------------------------------------------------------------
def log_scores(idx, score, dfz):
    """Print statistial analysis for single (pair, freq, period).
    """
    from datetime import timedelta as tdelta

    idx_dict = dict(zip(['pair','freq', 'period'], idx))
    freq = freqtostr[idx_dict['freq']]
    prd = pertostr[idx_dict['period']]
    candle = candles.newest(idx_dict['pair'], freq)
    open_time = to_local(candle['open_time'])
    close_time = to_local(candle['close_time'])
    prd_end = open_time - tdelta(microseconds=1)

    if freq == '5m':
        prd_start = open_time - tdelta(minutes=60)
    elif freq == '1h':
        prd_start = open_time - tdelta(hours=24)
    elif freq == '1d':
        prd_start = open_time - tdelta(days=7)

    siglog('-'*80)
    siglog(idx_dict['pair'])
    siglog("{} Candle:    {:%m-%d-%Y %I:%M%p}-{:%I:%M%p}".format(
        freq, open_time, close_time))
    if prd_start.day == prd_end.day:
        siglog("{} Hist:     {:%m-%d-%Y %I:%M%p}-{:%I:%M%p}".format(
            prd, prd_start, prd_end))
    else:
        siglog("{} Hist:     {:%m-%d-%Y %I:%M%p} - {:%m-%d-%Y %I:%M%p}".format(
            prd, prd_start, prd_end))
    siglog('')
    lines = dfz.to_string(index=False, col_space=10, line_width=100).title().split("\n")
    [siglog(line) for line in lines]
    siglog('')
    siglog("Mean Zscore: {:+.1f}".format(score))
    siglog('-'*80)
