# tests/testing.py
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
from pprint import pprint, pformat
from docs.conf import trade_strategies
import app.bot.strategy
import importlib
import pandas as pd
import numpy as np
import app

pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
app.set_db(["localhost", "45.79.176.125"][0])
db = app.get_db()

from app.bot import candles, macd, trade

trade.init(strategies=['macd_5m'])
trade.update('5m')

#candles.update('BTCUSDT', '5m') #, start=start_str, force=True)
#app.bot.dfc = candles.merge_new(pd.DataFrame(), ['BTCUSDT'],
#    span=now()-parse(start_str))
#df_macd = macd.generate(app.bot.dfc.loc[pair, freq])

