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


#from bitex.interface import Binance, Bitfinex, Bittrex, Bitstamp, CCEX, CoinCheck
#from bitex.interface import Cryptopia, HitBTC, Kraken, OKCoin, Poloniex, QuadrigaCX
#from bitex.interface import TheRockTrading, Vaultoro
from bitex import QuadrigaCX, Bitfinex, Binance
def binance_tckr(pair):
    binance = Binance()
    pair_str = pair[0] + pair[1]
    #print(binance._get_supported_pairs())
    raw  = binance.ticker(pair_str).text
    ticker = loads(raw)
    ticker["openTime"] = datetime.utcfromtimestamp(ticker["openTime"]/1000)
    ticker["closeTime"] = datetime.utcfromtimestamp(ticker["closeTime"]/1000)
    return ticker

t1 = Timer()
set_db(hosts[0])
log.debug("Set db in %s ms", t1)
db = get_db()
symbols=["ETC","LTC","NEO","GAS","BTC","ICX","DRGN","BTC","OMG","BCH","NANO","LINK","XMR"]
rng_1d_hourly = pd.date_range(utc_datetime()-timedelta(days=1), periods=24, freq='1H')
start=utc_dtdate() - timedelta(days=7)
end=utc_datetime()


