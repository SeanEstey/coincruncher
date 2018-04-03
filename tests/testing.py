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
from app.bnc import *
from app.bnc import analyze, candles, signals, trade, strategy
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
trade.init()
dfc = app.bnc.dfc
trade.freq = 60
trade.freq_str = '1m'
candle = candles.newest('BTCUSDT', '1m', df=app.bnc.dfc)


btc = dfc.loc['BTCUSDT',300]
n_fast=12
n_slow=26

EMAfast = btc['close'].ewm(span=n_fast, min_periods=n_slow - 1, adjust=True, ignore_na=False).mean()
EMAslow = btc['close'].ewm(span=n_slow, min_periods=n_slow - 1, adjust=True, ignore_na=False).mean()

MACD = pd.Series(EMAfast - EMAslow, name='MACD_{}_{}'.format(n_fast, n_slow))

MACDsign = MACD.ewm(span=9, min_periods=8, adjust=True, ignore_na=False).mean()
MACDsign.name = 'MACDsign_{}_{}'.format(n_fast, n_slow)

MACDdiff = pd.Series(MACD - MACDsign, name = 'MACDdiff_' + str(n_fast) + '_' + str(n_slow))

btc = btc.join(MACD)
btc = btc.join(MACDsign)
btc = btc.join(MACDdiff)
