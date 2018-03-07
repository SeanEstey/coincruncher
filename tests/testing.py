# tests/testing.py
import logging, time
from pprint import pprint
from importlib import reload
from datetime import timedelta
import pandas as pd
import numpy as np
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import app
from app.timer import Timer
from app.utils import utc_datetime, utc_dtdate
from app import signals

log = logging.getLogger("testing")
pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
hosts = ["localhost", "45.79.176.125"]
app.set_db(hosts[0])
db = app.get_db()
dfp = signals.load_db_pairs()
dfa = signals.load_db_aggregate()
