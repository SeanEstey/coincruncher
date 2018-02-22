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
