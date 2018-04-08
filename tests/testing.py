# tests/testing.py

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import logging, time
from pprint import pprint, pformat
import importlib
from datetime import timedelta as delta, datetime
import pandas as pd
import numpy as np
from pymongo import ReplaceOne, UpdateOne
from docs.config import *
from docs.rules import STRATS as rules
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
candle=None
scores=None

#------------------------------------------------------------------------------
def prep_chart():
    macd = signals.macd(asset, macd_sh, macd_ln).tail(periods)
    asset = asset.tail(periods)
    diff = macd['macd_diff']
    buy_vol = macd['volume'] * macd['buy_ratio']
    vema = buy_vol.ewm(span=3).mean()
    results = scanner.macd_analysis(pair, freqtostr[freq], periods)

# Conf
pair='BTCUSDT'
freqstr = '5m'
pdfreqstr = '5T'
periods = 288
unit = 'minutes'
freq = strtofreq[freqstr]
td = delta(minutes=periods*5)

candles.update([pair], freqstr,
    start = "{} {} ago utc".format(periods, unit),
    force=True)
app.bot.dfc = candles.merge_new(pd.DataFrame(), [pair],
    span=td * 2)
macd = signals.macd(app.bot.dfc.loc[pair, freq],
    rules['macd']['fast_span'], rules['macd']['slow_span'])
scan_res = scanner.macd_analysis(pair, freqstr, periods, pdfreqstr=pdfreqstr)
