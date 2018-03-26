import logging
from pymongo import UpdateOne, ReplaceOne
from datetime import datetime, timedelta as delta
from pprint import pprint
import pytz
import pandas as pd
import numpy as np
import app
from app import freqtostr, pertostr, strtofreq, strtoper, candles
from app.timer import Timer
from app.utils import to_local, to_relative_str, utc_datetime as now
from docs.config import Z_FACTORS
from docs.rules import RULES
def siglog(msg): log.log(100, msg)
log = logging.getLogger('signals')

#-----------------------------------------------------------------------------
def pct_mkt_change(dfc, freq_str, span=None, label=None):
    """Aggregate percent market change in last 1 minute.
    # FIXME: only pull up to date candle data. slice by date
    # to make sure no other data is distorting the results.
    """
    freq = strtofreq[freq_str]

    span = span if span else 2
    group = dfc.xs(freq, level=1).groupby('pair')

    if span == 2:
        pct_mkt = group.tail(2).groupby('pair').pct_change().mean()
    elif span > 2:
        pct_mkt = group.tail(span).dropna().groupby('pair').agg(
            lambda x: ((x.iloc[-1] - x.iloc[0]) / x.iloc[0])*100
        ).mean()

    df = pd.DataFrame(pct_mkt, columns=[label]).round(4)[0:1]
    df.index = ['Pct Change']
    return df

#-----------------------------------------------------------------------------
def z_score(dfc, candle):
    """Generate Mean/STD/Z-Score for given candle properties.
    Attempts to correct distorted Mean values in bearish/bullish markets by
    adjusting length of historic period. Perf: ~20ms
    Returns: pd.DataFrame w/ [5 x 4] dimensions
    """
    t1 = Timer()
    period = 1.0
    co,cf = candle['open_time'], candle['freq']

    if cf == '1m':
        end = co - delta(minutes=1)
        start = end - delta(hours = 2 * period)
    elif cf == '5m':
        end = co - delta(minutes=5)
        start = end - delta(hours = 2 * period)
    elif cf == '1h':
        end = co - delta(hours=1)
        start = end - delta(hours = 72 * period)

    history = dfc.loc[slice(start, end)]

    # Smooth signal/noise ratio with EMA.
    ema = history.ewm(span=5).mean()

    # Mean and SD
    stats = ema.describe()
    cols = ['close', 'volume', 'buy_ratio']

    # Calc Z-Scores
    data = [
        (candle['close'] - stats['close']['mean']) / stats['close']['std'],
        (candle['volume'] - stats['volume']['mean']) / stats['volume']['std'],
        (candle['buy_ratio'] - stats['buy_ratio']['mean']) / stats['buy_ratio']['std']
    ]

    return pd.Series(data, index=cols).astype('float64').round(8)

#------------------------------------------------------------------------------
def weighted_avg(df, col):
    """_df = grp.loc[grp['OPEN_TIME'] > now() - delta(hours=4)]
    _df = _df.select_dtypes(include=[np.number])
    _df['weights'] = np.linspace(0, 1, len(_df))
    pd_avg = (np.array(w) * pandas.DataFrame(a)).mean(axis=1)
    df = dfc.xs(60,level=1).reset_index().groupby('PAIR').apply(linseq)
    """
    weights = []
    try:
        return (df[col] * weights).sum() / sum(weights)
    except Exception as e:
        log.error("Div/0 error. Returning unweighted mean.")
        return df[col].mean()

#-----------------------------------------------------------------------------
def thresh_adapt():
    # ********************************************************************
    # TODO: Optimize by trying to group period logically via new "settled"
    # price range, if possible.
    # If bull/bear market, shorten historic period range
    #if abs(mkt_ma) > 0.05 and abs(mkt_ma) < 0.1:
    #    shorten = 0.75
    #elif abs(mkt_ma) >= 0.1 and abs(mkt_ma) < 0.15:
    #    shorten = 0.6
    #elif abs(mkt_ma) > 0.15:
    #    shorten = 0.5
    #
    # Correct for distorted Z-Score values in sudden bearish/bullish swings.
    # A volatile bearish swing pushes Z-Scores downwards faster than the mean
    # adjusts to represent the new "price level", creating innacurate deviation
    # values for Z-Scores. Offset by temporarily lowering support threshold.
    #
    # A) Breakout (ZP > Threshold)
    #   breakout = rules['Z-SCORE']['BUY_BREAK_REST']
    #   if z_score > breakout:
    #       msg="{:+.2f} Z-Score > {:.2f} Breakout.".format(z_score, breakout)
    #    return open_holding(candle, scores, extra=msg)
    # ********************************************************************
    #z_thresh = RULES[freq_str]['Z-SCORE']['THRESH']
    # Example: -0.1% MA and -2.0 support => -3.0 support
    if mkt_ma < 0:
        return z_thresh * (1 + (abs(mkt_ma) * 5))
    # Example: +0.1% MA and +2.0 support => +0.83 support
    else:
        return z_thresh * (1 - (mkt_ma * 1.75))

#------------------------------------------------------------------------------
def pca(df):
    """Run Principal Component Analysis (PCA) to identify time-lagged
    price correlations between coins.
    Code author: Dr. Pawel Lachowicz
    Source: https://tinyurl.com/yaswrf9u
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
def thresholding_algo(y, lag, threshold, influence):

    signals = np.zeros(len(y))
    filteredY = np.array(y)
    avgFilter = [0]*len(y)
    stdFilter = [0]*len(y)
    avgFilter[lag - 1] = np.mean(y[0:lag])
    stdFilter[lag - 1] = np.std(y[0:lag])

    for i in range(lag, len(y)):
        if abs(y[i] - avgFilter[i-1]) > threshold * stdFilter [i-1]:
            if y[i] > avgFilter[i-1]:
                signals[i] = 1
            else:
                signals[i] = -1

            filteredY[i] = influence * y[i] + (1 - influence) * filteredY[i-1]
            avgFilter[i] = np.mean(filteredY[(i-lag):i])
            stdFilter[i] = np.std(filteredY[(i-lag):i])
        else:
            signals[i] = 0
            filteredY[i] = y[i]
            avgFilter[i] = np.mean(filteredY[(i-lag):i])
            stdFilter[i] = np.std(filteredY[(i-lag):i])

    return dict(signals = np.asarray(signals),
                avgFilter = np.asarray(avgFilter),
                stdFilter = np.asarray(stdFilter))
