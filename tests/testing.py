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
from docs.botconf import trade_pairs

pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
app.set_db(["localhost", "45.79.176.125"][0])
db = app.get_db()
from app.bot import candles, macd, scanner, trade


pair='WANBTC'
data = [
    ('30m','30T',8),
    ('5m','5T',96),
    ('1h','1H',8)
]


trade.init()
results=[]
for i in range(0,len(data)):
    results.append(macd.agg_describe(
        pair, data[i][0], data[i][2], pdfreqstr=data[i][1]))

for r in results:
    print(r['summary'])
