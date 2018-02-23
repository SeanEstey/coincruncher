# app.pca
import logging
import numpy as np
import pandas as pd
from scipy import stats

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
