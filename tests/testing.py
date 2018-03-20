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

#------------------------------------------------------------------------------
trades.X_THRESH = 0.25
trades.preload_candles()
dfc = trades.dfc

candle = trades.last_candle('BNBUSDT','5m')
candle['CLOSE'] = 0.1
holding = db.trades.find_one({"pair":"BNBUSDT", "status":"open"})

trades.update('5m')
