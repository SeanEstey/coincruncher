import logging
from pymongo import ReplaceOne
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import app
from app import freqtostr, pertostr, strtofreq, strtoper, candles
from app.timer import Timer
from app.utils import to_local
from docs.config import Z_WEIGHTS, Z_FACTORS, Z_DIMEN, Z_IDX_NAMES
def siglog(msg): log.log(100, msg)
log = logging.getLogger('signals')

#------------------------------------------------------------------------------
def xscores(dfz):
    """Derive x-scores from z-scores dataset.
    """
    scores = []
    dfx = dfz.copy().xs('ZSCORE', level=3)

    # Apply weightings
    for idx, row in dfx.iterrows():
        scores.append((row * Z_WEIGHTS).sum() / sum(Z_WEIGHTS))

    # Mean zscore across close, volume, buy_ratio
    dfx['ZSCORE'] = dfx.mean(axis=1)
    # Add new column
    dfx['XSCORE'] = scores
    # Hide all other columns
    dfx = dfx[['XSCORE']]
    return dfx.sort_index()
#-----------------------------------------------------------------------------
def zscore(pair, freq, period, start, end, candle):
    """Assign Z-Scores for given candle using period historical average.
    Can be appended with other pair/freq/periods for easy indexing.
    Returns:
        pd.DataFrame of [4x4 INDEX][3x1 DATA] dimensions.
        Example index:
        PAIR      FREQ  PERIOD  DIMEN
        BTCUSDT-->300-->3600--> CANDLE
        `                  |--> MEAN
        `                  |--> STD
        `                  |--> ZSCORE
    """
    data = []
    # Historical average
    dfh_avg = candles.load_db(pair, freq, start, end=end).describe()[1::]

    # Stat/z-score data for Close, Volume, BuyRatio
    for x in Z_FACTORS:
        x = x.lower()
        data.append([
            candle[x],
            dfh_avg[x]['mean'],
            dfh_avg[x]['std'],
            (candle[x] - dfh_avg[x]['mean']) / dfh_avg[x]['std']
        ])

    idx_levels = [ [pair], [strtofreq[freq]], [strtoper[period]], Z_DIMEN ]
    dfz = pd.DataFrame(np.array(data).transpose(),
        index = pd.MultiIndex.from_product(idx_levels, names=Z_IDX_NAMES),
        columns = Z_FACTORS
    )
    # Small number precision
    dfz["CLOSE"] = dfz["CLOSE"].astype('float64')
    return dfz
#------------------------------------------------------------------------------
def zscores(pairs):
    """Build dataframe with scores for 5m, 1h, 1d frequencies across various
    time periods.
    """
    t1 = Timer()
    _1m = timedelta(minutes=1)
    _1h = timedelta(hours=1)
    _1d = timedelta(hours=24)
    dfz = pd.DataFrame()

    # Calculate z-scores for various (pair, freq, period) keys
    for pair in pairs:
        c5m = candles.last(pair,"5m")
        t5m = [c5m["open_time"] - (5*_1m), c5m["close_time"] - (5*_1m)]
        c1h = candles.last(pair,"1h")
        t1h = [c1h["open_time"] - (1*_1h), c1h["close_time"] - (1*_1h)]
        c1d = candles.last(pair,"1d")
        t1d = [c1d["open_time"] - (1*_1d), c1d["close_time"] - (1*_1d)]

        for n in range(1,4):
            dfz = dfz.append([
                zscore(pair, "5m", str(n*60)+"m", t5m[0]-(n*60*_1m), t5m[1], c5m),
                zscore(pair, "1h", str(n*24)+"h", t1h[0]-(n*24*_1h), t1h[1], c1h),
                zscore(pair, "1d", str(n*7)+"d",  t1d[0]-(n*7*_1d), t1d[1], c1d)
            ])

    log.debug("[%s rows x %s cols] z-score dataset built. [%ss]",
        len(dfz), len(dfz.columns), t1.elapsed(unit='s'))
    return dfz.sort_index()
#------------------------------------------------------------------------------
def print(idx, score, dfz):
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
    lines = dfz.to_string(col_space=10, line_width=100).title().split("\n")
    [siglog(line) for line in lines]
    siglog('')
    siglog("Mean Zscore: {:+.1f}".format(score))
    siglog('-'*80)
#-----------------------------------------------------------------------------
def save_db(dfz):
    """Given pair signal dataframe, Generate aggregate (sum) signals on each
    index (pair, freq, period, prop), along with time since last sign change,
    t(signal > 0) and t(signal < 0).
    """
    db = app.get_db()
    t1 = Timer()
    ops=[]

    for idx, row in dfz.iterrows():
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
    curs = app.get_db().zscores.find({"pair":pair, "freq":freq, "period":period})

    if curs.count() == 0:
        return None

    return pd.DataFrame(list(curs),
        index = pd.Index(Z_DIMEN, name='stats'),
        columns = Z_FACTORS
    ).astype('float64')
