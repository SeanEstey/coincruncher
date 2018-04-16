# tests/testing.py
import os,sys,inspect
currentdir = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
from dateparser import parse
from datetime import datetime, timedelta
from pprint import pprint
import importlib
import pandas as pd
import numpy as np
from binance.client import Client
from docs.botconf import trade_pairs
import app
from app.common.utils import utc_datetime as now
from app.common.timeutils import freqtostr, strtofreq
from app.bot import candles, macd, scanner, trade, tickers

trade_pairs = [
    'ADABTC',
    'AIONBTC',
    'BNBBTC',
    'BTCUSDT',
    'DGDBTC',
    'DNTBTC',
    'ELFBTC',
    'ETHUSDT',
    'FUNBTC',
    'EOSBTC',
    'ENJBTC',
    'ICXBTC',
    'HSRBTC',
    'LRCBTC',
    'OMGBTC',
    'POWRBTC',
    'ONTBTC',
    'OSTBTC',
    'SALTBTC',
    'STEEMBTC',
    'SUBBTC',
    'XVGBTC',
    'WABIBTC',
    'WANBTC',
    'WTCBTC',
    'ZILBTC'
]
pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
app.set_db(["localhost", "45.79.176.125"][0])
db = app.get_db()

#---------------------------------------------------------------------------
def histo_hist(df, pair, freqstr, startstr, periods):
    df = df.loc[pair, strtofreq(freqstr)]
    return macd.histo_phases(df, pair, freqstr, periods)

#---------------------------------------------------------------------------
def test_getphase(df, start_idx, pair, freq, periods):
    dfmacd = macd.generate(df).tail(periods)['macd_diff']
    return macd.get_phase(dfmacd, freq, start_idx)

#---------------------------------------------------------------------------
def load(pair, freqstr, startstr):
    candles.update([pair], freqstr, start=startstr, force=True)
    df = candles.load([pair], freqstr=freqstr, startstr=startstr)
    return df.loc[pair,strtofreq(freqstr)]


#df = load('ZILBTC','30m','72 hours ago utc')
#dfh, phases = macd.histo_phases(df, 'ZILBTC', '30m', 144)

#client = Client("","")
