import logging
from pymongo import UpdateOne, ReplaceOne
from datetime import datetime, timedelta
from pprint import pprint
import pytz
import pandas as pd
import numpy as np
import app
from app import freqtostr, pertostr, strtofreq, strtoper, candles
from app.timer import Timer
from app.utils import to_local, to_relative_str, utc_datetime as now
from docs.config import Z_FACTORS, Z_DIMEN, Z_IDX_NAMES
from docs.rules import RULES
def siglog(msg): log.log(100, msg)
log = logging.getLogger('signals')

#-----------------------------------------------------------------------------
def pct_market_change(dfc, freq_str):
    """Aggregate percent market change in last 1 minute.
	In [17]: idx = pd.IndexSlice

	# this is multi-index slicing
	# this will *always* work
	In [18]: df.loc[idx['2014-01-01',:], 'Col1'] += 1
    """
    freq = strtofreq[freq_str]

    pct_mkt = dfc.xs(freq, level=1).groupby('PAIR').pct_change().tail(1).mean()
    df = pd.DataFrame(pct_mkt, columns=['']).round(4)

    df = df.rename(index={
        'CLOSE':'Close', 'OPEN':'Open', 'VOLUME':'Volume', 'TRADES':'Trades', 'BUY_RATIO':'Buy Ratio'
    })

    if df.loc['Close'][0] < 0:
        sentiment = 'Bearish'
    else:
        sentiment = 'Bullish'

    dt = dfc.xs(freq,level=1).iloc[-1].name[1] + timedelta(minutes=1)
    dt = dt.replace(tzinfo=pytz.utc)
    rel_str = to_relative_str(now() - dt) + " ago"

    df.index.name = "('{}' Market, {}, {})".format(freq_str, rel_str, sentiment)

    log.info("")
    lines = df.to_string(formatters={df.columns[0]: '{:+,.4f}%'.format}).split("\n")
    [ log.info(x) for x in lines ]
    log.info("")

    return df

#-----------------------------------------------------------------------------
def z_score(dfc, candle, mkt_ma=None):
    """Generate Mean/STD/Z-Score for given candle properties.
    Attempts to correct distorted Mean values in bearish/bullish markets by
    adjusting length of historic period.
    Performance: ~20ms
    Returns:
        pd.DataFrame w/ [5 x 4] dimensions
    """
    t1 = Timer()

    # ********************************************************************
    # TODO: Optimize by trying to group period logically via new "settled"
    # price range, if possible.
    # ********************************************************************

    # If bull/bear market, shorten historic period range
    mkt_ma = mkt_ma if mkt_ma else 0
    shorten = 1.0
    #if abs(mkt_ma) > 0.05 and abs(mkt_ma) < 0.1:
    #    shorten = 0.75
    #elif abs(mkt_ma) >= 0.1 and abs(mkt_ma) < 0.15:
    #    shorten = 0.6
    #elif abs(mkt_ma) > 0.15:
    #    shorten = 0.5

    if candle['FREQ'] == '1m':
        hist_end = candle['OPEN_TIME'] - timedelta(minutes=1)
        hist_start = hist_end - timedelta(hours = 2 * shorten)
    elif candle['FREQ'] == '5m':
        hist_end = candle['OPEN_TIME'] - timedelta(minutes=5)
        hist_start = hist_end - timedelta(hours = 2 * shorten)
    elif candle['FREQ'] == '1h':
        hist_end = candle['OPEN_TIME'] - timedelta(hours=1)
        hist_start = hist_end - timedelta(hours = 72 * shorten)

    history = dfc.loc[slice(hist_start, hist_end)]

    # Use exponential moving average for higher weighting to
    # more recent prices to improve Z-Score
    history = history.ewm(span=5).mean()

    stats = history.describe()
    data = []

    # Insert mean/std/z-score etc for each column
    for x in Z_FACTORS:
        data.append([
            candle[x],
            stats[x]['mean'],
            stats[x]['std'],
            (candle[x] - stats[x]['mean']) / stats[x]['std']
        ])

    df = pd.DataFrame(np.array(data).transpose(),
        index=pd.Index(Z_DIMEN), columns=Z_FACTORS
    ).astype('float64').round(8)

    log.debug('Scores generated [{:,.0f}ms]'.format(t1))
    return df

#------------------------------------------------------------------------------
def weighted_avg(df, col):
    """
    _df = grp.loc[grp['OPEN_TIME'] > now() - timedelta(hours=4)]
    _df = _df.select_dtypes(include=[np.number])
    _df['weights'] = np.linspace(0, 1, len(_df))

    pd_avg = (np.array(w) * pandas.DataFrame(a)).mean(axis=1)

    df = dfc.xs(60,level=1).reset_index().groupby('PAIR').apply(linseq)
    """
    weights = []

    # Apply weightings
    try:
        return (df[col] * weights).sum() / sum(weights)
    except Exception as e:
        log.error("Div/0 error. Returning unweighted mean.")
        return df[col].mean()

#------------------------------------------------------------------------------
def adjust_support_level(freq_str, mkt_ma):
    """Correct for distorted Z-Score values in sudden bearish/bullish swings.
    A volatile bearish swing pushes Z-Scores downwards faster than the mean
    adjusts to represent the new "price level", creating innacurate deviation
    values for Z-Scores. Offset by temporarily lowering support threshold.
    """
    return RULES[freq_str]['Z-SCORE']['BUY_SUPT']

    # Example: -0.1% MA and -2.0 support => -3.0 support
    if mkt_ma < 0:
        return RULES[freq_str]['Z-SCORE']['BUY_SUPT'] * (1 + (abs(mkt_ma) * 5))
    # Example: +0.1% MA and +2.0 support => +0.83 support
    else:
        return RULES[freq_str]['Z-SCORE']['BUY_SUPT'] * (1 - (mkt_ma * 1.75))

#------------------------------------------------------------------------------
def adjust_support_margin(freq_str, mkt_ma):
    """If bought as a bounce trade, allow some wiggle room for price to bottom
    out before bouncing up.
    ADDME: category field in holding to avoid this hack.
    ADDME: adjust value closer to 1.0 in bearish markets, closer to 1.01 in
    bull markets, to optimize risk/reward.
    """
    margin = RULES[freq_str]['Z-SCORE']['SELL_SUPT_MARG']

    if mkt_ma > 0:
        return margin
    else:
        return 1.0

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
