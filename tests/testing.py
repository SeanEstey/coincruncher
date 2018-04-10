# tests/testing.py
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
from dateparser import parse
from pprint import pprint
import importlib
import pandas as pd
import numpy as np
import app
from app.common.utils import utc_datetime as now

pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
app.set_db(["localhost", "45.79.176.125"][0])
db = app.get_db()

from app.bot import candles, macd, trade

trade.init()
trade.update('5m')

#candles.update('BTCUSDT', '5m') #, start=start_str, force=True)
#app.bot.dfc = candles.merge_new(pd.DataFrame(), ['BTCUSDT'],
#    span=now()-parse("2 hours ago utc"))
#df_macd = macd.generate(app.bot.dfc.loc['BTCUSDT', 300])

