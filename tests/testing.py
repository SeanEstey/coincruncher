# tests/testing.py
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from dateparser import parse
from datetime import datetime, timedelta
from pprint import pprint
import importlib
import pandas as pd
import numpy as np
import app
from app.common.utils import utc_datetime as now
from app.common.timeutils import freqtostr, strtofreq
from docs.botconf import trade_pairs

pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
app.set_db(["localhost", "45.79.176.125"][0])
db = app.get_db()
from app.bot import candles, macd, scanner, trade

trade.init()
#df = scanner.scan("1h", 8, 20, idx_filter='BTC', quiet=True)
df2 = scanner.scan("30m", 16, 20, idx_filter='BTC', quiet=True)

def macd_analysis():
    pair='WANBTC'
    results=[]
    for n in [('30m', 8), ('5m', 96), ('1h', 8)]:
        results.append(macd.agg_describe(pair, n[0], n[1]))
    for r in results:
        print(r['summary'])
