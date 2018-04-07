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
from docs.config import *
from docs.data import *
import app
from app import freqtostr, strtofreq, pertostr
from app.common.timer import Timer
from app.common.utils import utc_datetime as now, utc_dtdate
from app.bot import *
from app.bot import scanner, candles, signals, trade, strategy
#from binance.client import Client

log = logging.getLogger("testing")
pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
app.set_db(["localhost", "45.79.176.125"][0])
db = app.get_db()
#dfc=None
candle=None
scores=None
#client=None
#tickers=None

#------------------------------------------------------------------------------
def get_histo(df, start_iloc):
    t1 = Timer()
    _df = df.iloc[start_iloc:]

    if start_iloc >= len(df)-1 or _df.iloc[0]['normalized'] == np.nan:
        return {}

    if _df.iloc[0]['normalized'] >= 0:
        sign = 'POSITIVE'
        next_idx = _df[_df['normalized'] < 0].head(1).index
    else:
        sign = 'NEGATIVE'
        next_idx = _df[_df['normalized'] >= 0].head(1).index

    if next_idx.empty:
        next_iloc = len(_df)
    else:
        next_iloc = _df.index.get_loc(next_idx[0])

    _df = _df.iloc[0 : next_iloc]

    return {
        'iloc': (start_iloc, start_iloc + next_iloc - 1),
        'length':len(_df),
        'seconds': (_df.index.freq.nanos / 1000000000) * len(_df),
        'sign':sign,
        'area': _df['normalized'].sum(),
        'df':_df,
        'calc_time_ms': t1.elapsed()
    }

#------------------------------------------------------------------------------
def macd_analysis(pair, freqstr, n_periods):
    from docs.rules import STRATS as rules
    from app.common.utils import to_relative_str
    from datetime import timedelta as delta
    from app.bot import pct_diff

    freq = strtofreq[freqstr]

    macd = signals.macd(
        app.bot.dfc.loc[pair, freq],
        rules['macd']['fast_span'],
        rules['macd']['slow_span']
    ).tail(n_periods).dropna().asfreq(freqstr.upper())

    histos=[]
    last_iloc = 0

    while last_iloc <= len(macd) - 1:
        histo = get_histo(macd, last_iloc+1)
        if not histo:
            break
        histos.append(histo)
        last_iloc = histo['iloc'][1]

    print("\n{} MACD Analysis".format(pair))
    print("Freq: {}, Phases: {}".format(
        macd.index.freq.freqstr, len(histos)))

    for sign in ['POSITIVE', 'NEGATIVE']:
        grp = [ n for n in histos if n['sign'] == sign ]

        area = np.array([ n['area'] for n in grp ])
        if len(area) == 0:
            area = np.array([0])

        periods = np.array([ n['length'] for n in grp ])
        if len(periods) == 0:
            periods = np.array([0])

        duration = np.array([ n['seconds'] for n in grp])
        if len(duration) > 0:
            mean_duration = to_relative_str(delta(seconds=duration.mean()))
        else:
            mean_duration = 0

        # Total percent gain in area
        price_diff = np.array([
            pct_diff(
                n['df'].iloc[0]['close'],
                n['df'].iloc[-1]['close']
            ) for n in grp
        ])

        print("{} Phases: {}\n"\
            "\tPrice: {:+.2f}%\n"\
            "\tTotal_Area: {:.2f}, Mean_Area: {:.2f},\n"\
            "\tTotal_Periods: {:}, Mean_Periods: {:.2f}\n"\
            "\tMean_Duration: {}"\
            .format(
                sign.title(),
                len(grp),
                price_diff.sum(),
                abs(area.sum()),
                abs(area.mean()),
                periods.sum(),
                periods.mean(),
                mean_duration))

    return histos

#------------------------------------------------------------------------------
trade.init()
dfc = app.bot.dfc
#n = macd_analysis('BTCUSDT','1h', 24)

