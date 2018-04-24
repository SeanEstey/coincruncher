# tests/testing.py
import os,sys,inspect
currentdir = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
from datetime import datetime, timedelta
from pprint import pprint
import importlib
import pytz
import pandas as pd
import numpy as np
from collections import OrderedDict as odict
from binance.client import Client
import app
from app.common.utils import utc_datetime as now, to_local
from app.common.timeutils import freqtostr, strtofreq

pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
app.set_db("localhost")
db = app.get_db()
from docs.botconf import *

from app.bot import candles, macd, scanner, trade, tickers

##### Init
app.bot.client = client = Client('','')

def histo_hist(df, pair, freqstr, startstr, periods):
    df = df.loc[pair, strtofreq(freqstr)]
    return macd.histo_phases(df, pair, freqstr, periods)

def test_getphase(df, start_idx, pair, freq, periods):
    dfmacd = macd.generate(df).tail(periods)['macd_diff']
    return macd.get_phase(dfmacd, freq, start_idx)

def trades():
    #pairs = app.bot.get_pairs()
    #app.bot.dfc = candles.bulk_load(pairs, TRD_FREQS)
    app.bot.init()
    pair = 'RLCBTC'
    freqstr = '1d'
    t = db.trades.find_one({'status':'open','pair':pair,'freqstr':freqstr})
    #app.bot.dfc = candles.bulk_load([pair], TRD_FREQS)
    #candles_ = candles.api_update([pair], [freqstr])
    #c = candles_[-1]
    #c['close'] = np.float64(c['close'])
    #c['closed'] = True
    #ss = trade.snapshot(c)
    #stats = trade.update_stats(t, ss)
