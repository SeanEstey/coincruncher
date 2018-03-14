# tests/testing.py

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import logging
import time
from pprint import pprint, pformat
from importlib import reload
from datetime import timedelta, datetime
import pandas as pd
import numpy as np
from pymongo import ReplaceOne, UpdateOne
import app
from app import candles, signals
from app.timer import Timer
from app.utils import utc_datetime as now, utc_dtdate

log = logging.getLogger("testing")
pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
hosts = ["localhost", "45.79.176.125"]
app.set_db(hosts[0])
db = app.get_db()

results = signals.update()
df_z = results[0]
df_wa = results[1]
df_out = results[2]
df_in = results[3]
