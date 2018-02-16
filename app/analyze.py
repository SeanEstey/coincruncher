# app.analyze

import logging
from pprint import pprint
import pandas as pd
import numpy as np
from scipy import stats
from app import get_db
from app.timer import Timer
from app.utils import parse_period, utc_dtdate
log = logging.getLogger('analyze')

#------------------------------------------------------------------------------
def top_symbols(rank):
    """Get list of ticker symbols within given rank.
    """
    db = get_db()
    _date = list(db.tickers_5m.find().sort("date",-1).limit(1))[0]["date"]
    cursor = db.tickers_5m.find({"date":_date, "rank":{"$lte":rank}}).sort("rank",1)
    return [n["symbol"] for n in list(cursor)]

#------------------------------------------------------------------------------
def price_matrix(symbols, start, end, freq):
    """Build price dataframe for list of symbols within date period.
    Returns the timeseries subset where all columns have price data. i.e. newly
    listed symbols with short price histories will force the entire subset to
    shrink significantly.
    """
    db = get_db()
    t0 = Timer()
    t1 = Timer()

    if freq == '1D':
        collname = 'tickers_1d'
        price = "$close"
    elif freq == '5T':
        collname = 'tickers_5m'
        price = "$price_usd"

    cursor = db[collname].aggregate([
        {"$match":{"symbol":{"$in":symbols}, "date":{"$gte":start, "$lt":end}}},
        {"$group":{
            "_id":"$symbol",
            "date":{"$push":"$date"},
            "price":{"$push":price}
        }}
    ])

    if not cursor:
        log.error("empty dataframe!")
        return []
    else:
        results = list(cursor)
        log.debug("queried %s results in %s ms.", t1.clock(t='ms'), len(results))
        t1.restart()

    # Empty dataframe w/ datetimeindex
    df = pd.DataFrame(index=pd.date_range(start=start, end=end, freq=freq))

    for result in results:
        _df = pd.DataFrame(
            columns=[result['_id']],
            index=result['date'],
            data=result['price'])
        _df = _df.resample(freq).mean().sort_index()
        df = df.join(_df)

    log.debug("isnull().sum(): %s", df.isnull().sum())

    df = df.dropna().round(2)

    log.debug("result dimensions: [{:,} rows x {:,} columns], t={:,}ms".format(
        len(df), len(df.columns), t0.clock(t='ms')))

    return df

#------------------------------------------------------------------------------
def corr_minmax(symbol, start, end, max_rank):
    """Find lowest & highest price correlation symbols (within max_rank) with
    given ticker symbol.
    """
    db = get_db()
    symbols = top_symbols(max_rank)
    df = price_matrix(symbols, start, end, '5T')

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
    price correlations between symbols. Code from:
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
