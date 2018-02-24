import logging, time
from importlib import reload
from json import loads
from pprint import pprint
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from app import get_db, set_db
from app import markets, cryptocompare, analyze, tickers, coinmktcap, utils
from app.timer import Timer
from app.utils import *

log = logging.getLogger("testing")
pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
hosts = ["localhost", "45.79.176.125"]
set_db(hosts[0])
db = get_db()
t1 = Timer()

def do():
    symbols=["ETC","LTC","NEO","GAS","BTC","ICX","DRGN","BTC","OMG","BCH","NANO","LINK","XMR"]
    rng_1d_hourly = pd.date_range(utc_datetime()-timedelta(days=1), periods=24, freq='1H')
    start=utc_dtdate() - timedelta(days=7)
    end=utc_datetime()

####### TEST CANDLES #########
from binance.client import Client
from app.candles import historical, to_df, store
pair = "NANOETH"
interval = Client.KLINE_INTERVAL_5MINUTE
start_str = "7 days ago UTC"
df = to_df(pair, historical(pair, interval, start_str))
df2 = df.resample("1H").mean()
print("***** %s Last 7 Days *****" % pair)
print(df2.ix[len(df2)-30::])
print("\n***** %s Summary *****\n" % pair)
print(df2.describe())
