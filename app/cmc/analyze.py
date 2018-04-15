# app.analyze
import logging
from pprint import pprint
from datetime import timedelta
import pandas as pd
import numpy as np
from scipy import stats
from app import get_db
from app.common.timer import Timer
from app.common.utils import parse_period, utc_dtdate
log = logging.getLogger('analyze')

_1DAY = timedelta(days=1)

#-----------------------------------------------------------------------------
def maxcorr(df, cols=None):
    maxdf = pd.concat(
        [df.replace(1.00,0).idxmax(), df.replace(1.00,0).max()], axis=1)
    maxdf.index.name="SYM1"
    maxdf.columns=cols or ["SYM2","CORR"]
    return maxdf

#-----------------------------------------------------------------------------
def maxcorr(coins, dt_rng):
    results=[]
    for ts in dt_rang:
        results.append(corr_5m(coins, ts.to_datetime().date()))
    pass

#-----------------------------------------------------------------------------
def corr_hourly(coins, _date):
    rng = pd.date_range(_date, periods=24, freq='1H')
    price_df(coins, rng).pct_change().corr().round(2)
    #_date-_1DAY, _date, '1H')

#-----------------------------------------------------------------------------
def corr_close(coins, date_rng):
    df = price_df(coins, date_rng, '1D').pct_change().corr().round(2)

#------------------------------------------------------------------------------
def price_df(coins, date_rng):
    """Build price dataframe for list of coins within date period.
    Returns the timeseries subset where all columns have price data. i.e. newly
    listed coins with short price histories will force the entire subset to
    shrink significantly.
    """
    db = get_db()
    t0,t1 = Timer(), Timer()
    freq = date_rng.freq
    dt0 = date_rng[0].to_datetime()
    dt1 = date_rng[-1].to_datetime()
    if freq.freqstr[-1] in ['D','M','Y']:
        collname = "cmc_tick"
        field = "$close"
    elif freq.freqstr[-1] in ['T','H']:
        collname = "cmc_tick"
        field = "$price_usd"

    cursor = db[collname].aggregate([
        {"$match":{
            "symbol":{"$in":coins},
            "date":{"$gte":dt0, "$lt":dt1}}},
        {"$group":{
            "_id":"$symbol",
            "date":{"$push":"$date"},
            "price":{"$push":field}}}])
    if not cursor:
        return log.error("empty dataframe!")

    coindata = list(cursor)

    df = pd.DataFrame(index=date_rng)

    for coin in coindata:
        df2 = pd.DataFrame(coin['price'], columns=[coin['_id']],index=coin['date']
            ).resample(freq).mean().sort_index()
        df = df.join(df2) #.resample(freq).mean().sort_index()

    n_drop = sum(df.isnull().sum())
    df = df.dropna().round(2)
    log.debug("price_df: frame=[{:,} x {:,}], dropped={:,}, t={}ms".format(
        len(df), len(df.columns), n_drop, t0))
    return df

#------------------------------------------------------------------------------
def corr_minmax(symbol, start, end, maxrank):
    """Find lowest & highest price correlation coins (within max_rank) with
    given ticker symbol.
    """
    db = get_db()
    coins = topcoins(maxrank)
    df = price_matrix(coins, start, end, '5T')

    if len(df) < 1:
        return {"min":None,"max":None}

    corr = df.corr()
    col = corr[symbol]
    del col[symbol]

    return {
        "symbol":symbol,
        "start":start,
        "end":end,
        "corr":col,
        "min": {col.idxmin(): col[col.idxmin()]},
        "max": {col.idxmax(): col[col.idxmax()]}
    }

#------------------------------------------------------------------------------
def corr_minmax_history(symbol, start, freq, max_rank):
    delta = parse_period(freq)[2]
    _date = start
    results=[]
    while _date < utc_dtdate():
        results.append(corr_minmax(symbol, _date, _date+delta, max_rank))
        _date += delta

    return results

#------------------------------------------------------------------------------
def topcoins(rank):
    """Get list of ticker symbols within given rank.
    """
    db = get_db()
    _date = list(db.cmc_tick.find().sort("date",-1).limit(1))[0]["date"]
    cursor = db.cmc_tick.find({"date":_date, "rank":{"$lte":rank}}).sort("rank",1)
    return [n["symbol"] for n in list(cursor)]

