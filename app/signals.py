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
from docs.config import Z_WEIGHTS, Z_FACTORS, Z_DIMEN, Z_IDX_NAMES
def siglog(msg): log.log(100, msg)
log = logging.getLogger('signals')

#-----------------------------------------------------------------------------
def generate(dfc, candle):
    """Generate Z-Scores and X-score.
    Performance: ~20ms
    """
    t1 = Timer()

    if candle['FREQ'] == '1m':
        hist_end = candle['OPEN_TIME'] - timedelta(minutes=1)
        hist_start = hist_end - timedelta(hours=1)
    elif candle['FREQ'] == '5m':
        hist_end = candle['OPEN_TIME'] - timedelta(minutes=5)
        hist_start = hist_end - timedelta(hours=1)
    elif candle['FREQ'] == '1h':
        hist_end = candle['OPEN_TIME'] - timedelta(hours=1)
        hist_start = hist_end - timedelta(hours=72)

    history = dfc.loc[slice(hist_start, hist_end)]
    stats = history.describe()
    data = []

    # Insert mean/std/z-score etc for each column
    for x in Z_FACTORS:
        data.append([
            candle[x],
            stats[x]['mean'],
            stats[x]['std'],
            (candle[x] - stats[x]['mean']) / stats[x]['std'],
            np.nan
        ])

    df = pd.DataFrame(np.array(data).transpose(),
        index=pd.Index(Z_DIMEN), columns=Z_FACTORS
    ).astype('float64').round(4)

    df.loc['XSCORE'] = (df.loc['ZSCORE'] * Z_WEIGHTS).round(4)

    log.debug('Scores generated [{:,.0f}ms]'.format(t1))

    return df
#------------------------------------------------------------------------------
def xscore(z_scores):
    """Derive x-scores from z-scores dataset.
    """
    return (z_scores * Z_WEIGHTS).sum() / sum(Z_WEIGHTS)
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
#-----------------------------------------------------------------------------
def save_db(dfz):
    """Append scoring data to existing candles db records for each (pair,freq)
    candle in dataframe.
    """
    t1 = Timer()
    ops=[]

    for idx in set([n[:-1] for n in dfz.index.values]):
        idx_dict = dict(zip(dfz.index.names, idx))
        idx_dict = {k.lower():v for k,v in idx_dict.items()}
        period = idx_dict['period']
        del idx_dict['period']
        idx_dict['freq'] = freqtostr[idx_dict['freq']]
        record = dfz.loc[idx].to_dict()
        record = {k.lower():v for k,v in record.items()}
        ops.append(UpdateOne(idx_dict, {'$set':{'signals.zscores.'+pertostr[period]:record}}))

    try:
        results = app.get_db().candles.bulk_write(ops)
    except Exception as e:
        return log.exception(str(e))

    log.debug("%s candle records updated w/ z-scores. [%sms]", results.modified_count, t1)
#-----------------------------------------------------------------------------
def load_db(pair, freq):
    """Load zscore data appended to candle db record.
    Returns:
        multi-index pd.DataFrame
        index levels: (PAIR, CLOSE_TIME, FREQ, PERIOD, DIMEN)
        columns: (CLOSE, VOLUME, BUY_VOL)
    """
    curs = app.get_db().candles.find({"pair":pair, "freq":freq}
        ).sort("close_time",-1).limit(1)
    if curs.count() == 0:
        return None

    candle = list(curs)[0]
    periods = [ strtoper[n] for n in candle['signals.zscores'].keys() ]
    zscores = candle['signals.zscores']
    data=[]
    for period in zscores.keys():
        x = candle['signals.zscores'][period]
        for dimen in Z_DIMEN:
            data.append(np.array([x['close'][dimen], x['volume'][dimen], x['buy_ratio'][dimen]]))

    levels = [[pair], [strtofreq[freq]], [strtoper[periods]], [candle['close_time']], Z_DIMEN]
    dfz = pd.DataFrame(data,
        index = pd.MultiIndex.from_product(levels, names=Z_IDX_NAMES),
        columns = Z_FACTORS
    )
    return dfz
