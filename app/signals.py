import logging
from pymongo import UpdateOne, ReplaceOne
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
def xscore(dfz):
    """Derive x-scores from z-scores dataset.
    """
    scores = []
    dfx = dfz.copy().xs('ZSCORE', level=4)

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
    @candle: dict
    Returns:
        pd.DataFrame of [5x4 INDEX][3x1 DATA] dimensions.
        e.g:
        PAIR      CLOSE_TIME  FREQ  PERIOD  DIMEN   CLOSE   VOLUME  BUY_RATIO
        BTCUSDT-->2018-03---->300-->3600--> CANDLE    ###      ###        ###
        `                              |--> MEAN      ###      ###        ###
        `                              |--> STD       ###      ###        ###
        `                              |--> ZSCORE    ###      ###        ###
    """
    # Historical average
    dfh_avg = candles.load_db(pair, freq, start, end=end).describe()[1::]

    # Stat/z-score data for Close, Volume, BuyRatio
    data = []
    for x in Z_FACTORS:
        x = x.lower()
        data.append([
            candle[x],
            dfh_avg[x]['mean'],
            dfh_avg[x]['std'],
            (candle[x] - dfh_avg[x]['mean']) / dfh_avg[x]['std']
        ])

    levels = [[pair], [strtofreq[freq]], [strtoper[period]], [candle['close_time']], Z_DIMEN]

    dfz = pd.DataFrame(np.array(data).transpose(),
        index = pd.MultiIndex.from_product(levels, names=Z_IDX_NAMES),
        columns = Z_FACTORS
    )
    # Small number precision
    dfz["CLOSE"] = dfz["CLOSE"].astype('float64')
    return dfz
#------------------------------------------------------------------------------
def log_scores(idx, score, dfz):
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
