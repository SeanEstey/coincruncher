# app.signals
import logging
from pymongo import ReplaceOne
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import app
from app import candles
from app.timer import Timer
from app.utils import utc_datetime as now, to_local
from docs.data import FREQ_STR as freqtostr, PER_STR as pertostr

log = logging.getLogger('signals')
strtofreq = dict(zip(list(freqtostr.values()), list(freqtostr.keys())))
strtoper = dict(zip(list(pertostr.values()), list(pertostr.keys())))
def siglog(msg): log.log(100, msg)

#------------------------------------------------------------------------------
def weight_scores(df_z, weights):
    """Check if any updated datasets have z-scores > 2, track in DB to determine
    price correlations.
    """
    wt_scores = []
    df_wt = df_z.copy().xs('zscore', level=3)

    for idx, row in df_wt.iterrows():
        wt_scores.append((row * weights).sum() / sum(weights))

    df_wt['mean'] = df_wt.mean(axis=1)
    df_wt['weighted'] = wt_scores

    return df_wt.sort_index()

#-----------------------------------------------------------------------------
def zscore(pair, freq, period, start, end, candle):
    """Measure deviation from the mean for this candle data across the given
    historical time period. Separate z-scores are assigned for each candle
    property.
        Z-Score of 1: within standard deviation
        Z-Score of 2.5: large deviation from mean, sign of trading pattern
        diverging sharply from current historical pattern.
    """
    data = []
    # Save as int to enable df index sorting
    _freq = strtofreq[freq]
    _period = strtoper[period]
    columns = ['close', 'open', 'volume', 'buy_vol', 'buy_ratio', 'trades']
    stats_idx = ['candle', 'mean', 'std', 'zscore']

    # Statistical data and z-score
    hist_data = candles.load_db(pair, freq, start, end=end)
    hist_avg = hist_data.describe()[1::]

    for x in columns:
        data.append([
            candle[x],
            hist_avg[x]['mean'],
            hist_avg[x]['std'],
            (candle[x] - hist_avg[x]['mean']) / hist_avg[x]['std']
        ])

    index = pd.MultiIndex.from_product(
        [[pair],[_freq],[_period], stats_idx],
        names=['pair','freq','period','stats']
    )

    # Reverse lists. x-axis: candle_fields, y-axis: stat_data
    df_h = pd.DataFrame(np.array(data).transpose(),
        index=index,
        columns=columns
    )

    # Enhance floating point precision for small numbers
    df_h["close"] = df_h["close"].astype('float64')
    return df_h

#-----------------------------------------------------------------------------
def save_db(df_z):
    """Given pair signal dataframe, Generate aggregate (sum) signals on each
    index (pair, freq, period, prop), along with time since last sign change,
    t(signal > 0) and t(signal < 0).
    """
    db = app.get_db()
    t1 = Timer()
    ops=[]

    for idx, row in df_z.iterrows():
        query = dict(zip(['pair', 'freq', 'period'], idx))
        record = query.copy()
        record.update(row.to_dict())
        ops.append(ReplaceOne(query, record, upsert=True))
    try:
        res = db.zscores.bulk_write(ops)
    except Exception as e:
        return log.exception(str(e))
    log.debug("%s pair signals saved. [%sms]", res.modified_count, t1)

#-----------------------------------------------------------------------------
def load_scores(pair, freq, period):
    """Load pair signal data from DB as multi-index dataframe.
    Returns:
        pair signals dataframe
        index levels: [pair, freq, period, indicator]
        columns: [candle, mean, std, signal]
    """
    curs = app.get_db().zscores.find(
        {"pair":pair, "freq":freq, "period":period})

    if curs.count() == 0:
        return None

    return pd.DataFrame(list(curs),
        index = pd.Index(['candle', 'mean', 'std', 'zscore'], name='stats'),
        columns = ['close', 'open', 'volume', 'buy_vol', 'buy_ratio', 'trades']
    ).astype('float64')

#------------------------------------------------------------------------------
def print_score(idx, score, df_z):
    """Print statistial analysis for single (pair, freq, period).
    """
    from datetime import timedelta as tdelta

    idx_dict = dict(zip(['pair','freq', 'period'], idx))
    freq = freqtostr[idx_dict['freq']]
    prd = pertostr[idx_dict['period']]
    candle = candles.last(idx_dict['pair'], freq)
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
    lines = df_z.to_string(col_space=10, line_width=100).title().split("\n")
    [siglog(line) for line in lines]
    siglog('')
    siglog("Mean Zscore: {:+.1f}".format(score))
    siglog('-'*80)

#------------------------------------------------------------------------------
def generate_dataset(pairs):
    """Build dataframe with scores for 5m, 1h, 1d frequencies across various
    time periods.
    """
    t1 = Timer()
    _1m = timedelta(minutes=1)
    _1h = timedelta(hours=1)
    _1d = timedelta(hours=24)
    df_data = pd.DataFrame()

    # Calculate z-scores for various (pair, freq, period) keys
    for pair in pairs:
        c5m = candles.last(pair,"5m")
        t5m = [c5m["open_time"] - (5*_1m), c5m["close_time"] - (5*_1m)]
        c1h = candles.last(pair,"1h")
        t1h = [c1h["open_time"] - (1*_1h), c1h["close_time"] - (1*_1h)]
        c1d = candles.last(pair,"1d")
        t1d = [c1d["open_time"] - (1*_1d), c1d["close_time"] - (1*_1d)]

        for n in range(1,4):
            df_data = df_data.append([
                zscore(pair, "5m", str(n*60)+"m", t5m[0]-(n*60*_1m), t5m[1], c5m),
                zscore(pair, "1h", str(n*24)+"h", t1h[0]-(n*24*_1h), t1h[1], c1h),
                zscore(pair, "1d", str(n*7)+"d",  t1d[0]-(n*7*_1d), t1d[1], c1d)
            ])

    log.debug("Generated [%s rows x %s cols] z-score dataframe from Binance candles. [%ss]",
        len(df_data), len(df_data.columns), t1.elapsed(unit='s'))
    return df_data.sort_index()
