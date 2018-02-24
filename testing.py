import logging, time
from importlib import reload
from json import loads
from pprint import pprint
from datetime import datetime, timedelta
import pandas as pd
from app import get_db, set_db
from app.timer import Timer
log = logging.getLogger("testing")

pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
hosts = ["localhost", "45.79.176.125"]
set_db(hosts[0])
db = get_db()
t1 = Timer()


# Get most recent candle
from datetime import timedelta
from app import candles, signals
from app.utils import utc_datetime
pair = "NANOETH"
candle = candles.get_new(pair)
hist_df = candles.get_historic(
    pair,
    utc_datetime() - timedelta(days=7),
    utc_datetime()
)

signals.sigstr(candle, hist_df)

"""pair = "NANOETH"
_list = candles.get(pair)
dfmin = pd.DataFrame(_list)
dfmin.index = dfmin["date"]
dfhr = dfmin.resample("1H").mean()
dfhr["trades"] = dfhr["trades"].astype(int)
for col in ["base_buy_vol", "quote_sell_vol", "low", "high", "close"]:
    dfhr[col+"_diff"] = dfhr[col].pct_change()*100
dfhr = dfhr.round(4).dropna()
dfhr = dfhr[[
    "trades", "base_buy_vol", "base_buy_vol_diff",
    "quote_sell_vol", "quote_sell_vol_diff"
    "low", "low_diff",
    "high", "high_diff",
    "close", "close_diff"
]]
#print("buyvol std: %s" % dfhr.describe()["base_buy_vol_diff"]["std"])
print("\n%s Hourly.Dataset\n" % pair)
print(dfhr)
print("\n%s Hourly.Describe\n" % pair)
print(dfhr.describe().ix[1::])
"""
