# tests/testing.py

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import logging, time
from pprint import pprint, pformat
import importlib
from datetime import timedelta, datetime
import pandas as pd
import numpy as np
from pymongo import ReplaceOne, UpdateOne
import app
from app import freqtostr, strtofreq, pertostr, candles, signals, trades
from app.timer import Timer
from app.utils import utc_datetime as now, utc_dtdate
from docs.config import *
from docs.data import *

log = logging.getLogger("testing")
pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
app.set_db(["localhost", "45.79.176.125"][0])
db = app.get_db()
dfc=None
candle=None
scores=None

"""# Auto-remove non-number columns:
    df.select_dtypes(include=[np.number])
# Stacked column indexes:
    pd.DataFrame({'stacked_idx_col1':df, 'stacked_idx_col2':df})
"""

#------------------------------------------------------------------------------
def init():
    global dfc, candle, scores
    trades.init()
    dfc = trades.dfc
    del dfc['ZSCORE_1M'], dfc['ZSCORE_5M'], dfc['ZSCORE_1H']#, dfc['TRADES']

    pair = 'BTCUSDT'
    candle = candles.newest(pair,'1m', df=trades.dfc)
    scores = signals.z_score(dfc.loc[pair,strtofreq['1m']], candle)
    #holding = db.trades.find_one({"pair":pair, "status":"open"})
    #trades.update('1m')


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

#------------------------------------------------------------------------------
def weighted_avg(grp):
    """1m freq * 4hrs = 240 periods
    """
    _df = grp.xs(60, level=1)
    _df = _df.iloc[len(_df)-240:-1]
    close = _df['CLOSE']
    w = np.linspace(start=0, stop=1, num=len(close))
    zscore = (close - (close * w).mean()) / close.std()
    pprint(len(zscore))
    return pd.DataFrame(
        {"close":close, "weight":w, "zscore":zscore},
        index = f[['PAIR','OPEN_TIME']]
    )

##### MAIN #####
init()



"""
np.random.seed(0)
df = pd.DataFrame({
    "Date": pd.date_range(start='2018-01-01', end='2018-01-03 18:00:00', freq='6H'),
    "Weight": np.random.uniform(3, 5, 12),
    "V1": np.random.uniform(10, 15, 12),
    "V2": np.random.uniform(10, 15, 12),
    "V3": np.random.uniform(10, 15, 12)
})

df.index = df["Date"]
df_agg = df.groupby(pd.Grouper(freq='1D')).apply(weighted_average_std).unstack(-1)
print(df_agg)
"""
