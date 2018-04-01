import logging
from datetime import timedelta as delta
import pandas as pd
import numpy as np
import app, app.bnc
from . import candles
from app import strtofreq, freqtostr
log = logging.getLogger('signals')

#-----------------------------------------------------------------------------
def vwap():
    # buy volume * price / total volume
    pass

#-----------------------------------------------------------------------------
def rsi(candle):
    #https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/RSI
    #RSI = 100 – [100 / ( 1 + (Average of Upward Price Change / Average of Downward Price Change ) ) ]
    pass

#-----------------------------------------------------------------------------
def ema_pct_change(candle):
    """Calculate percent change of candle 'close' price exponential moving
    average for preset time span.
    """
    span = app.bnc.rules['EMA']['SPAN']

    # Convert span to time range
    _range = {
        '1m': delta(minutes = span * 2),
        '5m': delta(minutes = 5 * span * 2),
        '1h': delta(hours = span * 2),
        '1d': delta(days = span * 2)
    }

    df = app.bnc.dfc.loc[candle['pair'], strtofreq[candle['freq']]]
    sliced = df.loc[slice(
        candle['open_time'] - _range[candle['freq']],
        candle['open_time']
    )]['close'].copy()

    df_ema = sliced.ewm(span=span).mean().pct_change() * 100
    df_ema.index = [ str(x)[:-10] for x in df_ema.index.values ]
    return df_ema.round(3)

#-----------------------------------------------------------------------------
def z_score(candle, periods):
    """Generate Mean/STD/Z-Score for given candle properties.
    Attempts to correct distorted Mean values in bearish/bullish markets by
    adjusting length of historic period. Perf: ~20ms
    Returns: pd.DataFrame w/ [5 x 4] dimensions
    """
    smoothen = app.bnc.rules['Z-SCORE']['SMOOTH_SPAN']
    df = app.bnc.dfc.loc[candle['pair'], strtofreq[candle['freq']]]

    co,cf = candle['open_time'], candle['freq']

    if cf == '1m':
        end = co - delta(minutes=1)
        start = end - delta(minutes = periods)
    elif cf == '5m':
        end = co - delta(minutes=5)
        start = end - delta(minutes = 5 * periods)
    elif cf == '1h':
        end = co - delta(hours=1)
        start = end - delta(hours = periods)

    history = df.loc[slice(start, end)]

    # Smooth signal/noise ratio with EMA.
    ema = history.ewm(span=smoothen).mean()

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
