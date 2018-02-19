# app.analyze

import logging
from pprint import pprint
from datetime import timedelta
import pandas as pd
import numpy as np
from scipy import stats
from app import get_db
from app.timer import Timer
from app.utils import parse_period, utc_dtdate
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
    collname = "tickers_1d" if freq in ['D','M','Y'] else "tickers_5m"
    field = "$close" if freq in ['D','M','Y'] else "$price_usd"

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
    log.debug("price_df: queried %s matches in %s ms.", t1, len(coindata))

    df = pd.DataFrame(index=date_rng)
    for coin in coindata:
        df = df.join(
            pd.DataFrame(coin['price'], columns=[coin['_id']], index=coin['date']
            ).resample(freq).mean().sort_index())

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
def pca(df):
    """Run Principal Component Analysis (PCA) to identify time-lagged
    price correlations between coins. Code from:
    http://www.quantatrisk.com/2017/03/31/cryptocurrency-portfolio-correlation-pca-python/
    """
    m = df.mean(axis=0)
    s = df.std(ddof=1, axis=0)

    # normalised time-series as an input for PCA
    dfPort = (df - m)/s

    c = np.cov(dfPort.values.T)     # covariance matrix
    co = np.corrcoef(df.values.T)  # correlation matrix

    tickers = list(df.columns)

    # perform PCA
    w, v = np.linalg.eig(c)

    # choose PC-k numbers
    k1 = -1  # the last PC column in 'v' PCA matrix
    k2 = -2  # the second last PC column

    # begin constructing bi-plot for PC(k1) and PC(k2)
    # loadings

    # compute the distance from (0,0) point
    dist = []
    for i in range(v.shape[0]):
        x = v[i,k1]
        y = v[i,k2]
        d = np.sqrt(x**2 + y**2)
        dist.append(d)

    # check and save membership of a coin to
    # a quarter number 1, 2, 3 or 4 on the plane
    quar = []
    for i in range(v.shape[0]):
        x = v[i,k1]
        y = v[i,k2]
        d = np.sqrt(x**2 + y**2)

        #if(d > np.mean(dist) + np.std(dist, ddof=1)):
        # THESE IFS WERE NESTED IN ABOVE IF CLAUSE
        if((x > 0) and (y > 0)):
            quar.append((i, 1))
        elif((x < 0) and (y > 0)):
            quar.append((i, 2))
        elif((x < 0) and (y < 0)):
            quar.append((i, 3))
        elif((x > 0) and (y < 0)):
            quar.append((i, 4))

    for i in range(len(quar)):
        # Q1 vs Q3
        if(quar[i][1] == 1):
            for j in range(len(quar)):
                if(quar[j][1] == 3):
                    # highly correlated coins according to the PC analysis
                    print(tickers[quar[i][0]], tickers[quar[j][0]])
                    ts1 = df[tickers[quar[i][0]]]  # time-series
                    ts2 = df[tickers[quar[j][0]]]
                    # correlation metrics and their p_values
                    slope, intercept, r2, pvalue, _ = stats.linregress(ts1, ts2)
                    ktau, kpvalue = stats.kendalltau(ts1, ts2)
                    print(r2, pvalue)
                    print(ktau, kpvalue)
        # Q2 vs Q4
        if(quar[i][1] == 2):
            for j in range(len(quar)):
                if(quar[j][1] == 4):
                    print(tickers[quar[i][0]], tickers[quar[j][0]])
                    ts1 = df[tickers[quar[i][0]]]
                    ts2 = df[tickers[quar[j][0]]]
                    slope, intercept, r2, pvalue, _ = stats.linregress(ts1, ts2)
                    ktau, kpvalue = stats.kendalltau(ts1, ts2)
                    print(r2, pvalue)
                    print(ktau, kpvalue)

#------------------------------------------------------------------------------
def topcoins(rank):
    """Get list of ticker symbols within given rank.
    """
    db = get_db()
    _date = list(db.tickers_5m.find().sort("date",-1).limit(1))[0]["date"]
    cursor = db.tickers_5m.find({"date":_date, "rank":{"$lte":rank}}).sort("rank",1)
    return [n["symbol"] for n in list(cursor)]

