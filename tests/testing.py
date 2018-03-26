# tests/testing.py

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import logging, time
from pprint import pprint, pformat
import importlib
from datetime import timedelta, datetime
import pandas as pd
import numpy as np
from pymongo import ReplaceOne, UpdateOne
import app
from app import freqtostr, strtofreq, pertostr
from app.bnc import candles, signals, trades
from app.timer import Timer
from app.utils import utc_datetime as now, utc_dtdate
from docs.config import *
from docs.data import *
from binance.client import Client

log = logging.getLogger("testing")
pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
app.set_db(["localhost", "45.79.176.125"][0])
db = app.get_db()
dfc=None
candle=None
scores=None
client=None
tickers=None

#------------------------------------------------------------------------------
def _unfinished():
    # *********************************************************************
    # Calculate Z-Scores, store in dataframe/mongodb
    # ops=[]
    # for pair in pairs:
    #    candle = candles.newest(pair, freq_str, df=dfc)
    #    scores = signals.z_score(
    #        dfc.loc[pair,freq], candle, mkt_ma=mkt_ma)
    #    name = 'ZSCORE_' + freq_str.upper()
    #   dfc[name].loc[pair,freq][-1] = scores['CLOSE']['ZSCORE'].round(3)
    #   ops.append(UpdateOne({"open_time":candle["OPEN_TIME"],
    #       "pair":candle["PAIR"], "freq":candle["FREQ"]},
    #       {'$set': {name: scores['CLOSE']['ZSCORE']}}
    #   ))
    #   db.candles.bulk_write(ops)
    #
    #   if c2['OPEN_TIME'] < c1['OPEN_TIME']:
    #       return False
    # *********************************************************************

    # ********************************************************************
    # A. Profit loss
    # if c2['CLOSE'] < c1['CLOSE']:
    #    if 'Resistance' not in holding['buy']['details']:
    #        return sell(holding, c2, scores)
    #    margin = signals.adjust_support_margin(freq_str, mkt_ma)
    #    if (c2['CLOSE'] * margin) < c1['CLOSE']:
    #        return sell(holding, c2, scores)
    # B. Maximize profit, make sure price still rising.
    # p_max = df.loc[slice(c1['OPEN_TIME'], df.iloc[-2].name)]['CLOSE'].max()
    # elif not np.isnan(p_max) and candle['CLOSE'] < p_max:
    #   return sell(holding, c2, scores)
    # ********************************************************************

    # ********************************************************************
    # Open Trades (If Sold at Present Value)
    # pct_change_hold = []
    # active = list(db.trades.find({'status':'open'}))
    # for hold in active:
    #    candle = candles.newest(hold['pair'], freq_str, df=dfc)
    #    pct_change_hold.append(pct_diff(hold['buy']['candle']['CLOSE'], candle['CLOSE']))
    #
    # if len(pct_change_hold) > 0:
    #     pct_change_hold = sum(pct_change_hold)/len(pct_change_hold)
    # else:
    #     pct_change_hold = 0.0
    #
    # siglog("Holdings: {} Open, {:+.2f}% Mean Value".format(len(active), pct_change_hold))
    # siglog('-'*80)
    # ********************************************************************
    pass

#-----------------------------------------------------------------------------
def print_tickers():
    # *********************************************************************
    # TODO: Create another trading log for detailed ticker tarding signals.
    # Primary siglog will be mostly for active trading/holdings.
    # *********************************************************************
    pass

#------------------------------------------------------------------------------
def init():
    global dfc, candle, scores
    trades.init()
    dfc = trades.dfc
    pair = 'BTCUSDT'
    candle = candles.newest(pair,'1m', df=trades.dfc)
    pprint(candle)
    scores = signals.z_score(dfc.loc[pair,strtofreq['1m']], candle)
    trades.freq = 60
    trades.freq_str = '1m'
    trades.update('1m')

def binance_tickers():
    global client, tickers
    client = Client("", "")
    t1 = Timer()
    tickers = client.get_all_tickers()
    print("tickers received in {}ms".format(t1))

##### MAIN #####
#init()
binance_tickers()
