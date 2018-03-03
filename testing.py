import logging, time
from pprint import pprint
from importlib import reload
from datetime import timedelta
import pandas as pd
import numpy as np
from app import get_db, set_db
from app.timer import Timer
from app.utils import utc_datetime, utc_dtdate
from app.candles import api_get, api_get_all, db_get
from app.signals import gsigstr, sigstr, _print

# Config
log = logging.getLogger("testing")
pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
hosts = ["localhost", "45.79.176.125"]
set_db(hosts[0])
db = get_db()


df = gsigstr(mute=True,dbstore=False)
print("Signals")
print(df["signalsum"])
#print("\nMax 5m Signal Pair")
#print(df["max5m"])
#print("\nMax 1h Signal Pair")
#print(df["max1h"])
