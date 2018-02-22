import logging, time
from importlib import reload
from json import loads
from pprint import pprint
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from app import get_db, set_db
from app import binance, markets, cryptocompare, analyze, tickers, coinmktcap, utils
from app.timer import Timer
from app.utils import *

log = logging.getLogger("testing")
pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
hosts = ["localhost", "45.79.176.125"]
db = get_db()
set_db(hosts[0])
t1 = Timer()
log.debug("Set db in %s ms", t1)

def do():
    symbols=["ETC","LTC","NEO","GAS","BTC","ICX","DRGN","BTC","OMG","BCH","NANO","LINK","XMR"]
    rng_1d_hourly = pd.date_range(utc_datetime()-timedelta(days=1), periods=24, freq='1H')
    start=utc_dtdate() - timedelta(days=7)
    end=utc_datetime()


from binance.client import Client
from app.binance import get_historical_klines

# fetch 1 minute klines for the last day up until now
""" [
    1499040000000,      # Open time
    "0.01634790",       # Open
    "0.80000000",       # High
    "0.01575800",       # Low
    "0.01577100",       # Close
    "148976.11427815",  # Volume
    1499644799999,      # Close time
    "2434.19055334",    # Quote asset volume
    308,                # Number of trades
    "1756.87402397",    # Taker buy base asset volume
    "28.46694368",      # Taker buy quote asset volume
    "17928899.62484339" # Ignore
  ]
"""
klines = get_historical_klines("NANOETH", Client.KLINE_INTERVAL_5MINUTE, "1 day ago UTC")






