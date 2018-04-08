# tests/testing.py
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
from dateparser import parse
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
from app.bot import *

log = logging.getLogger("testing")
pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
app.set_db(["localhost", "45.79.176.125"][0])
db = app.get_db()



