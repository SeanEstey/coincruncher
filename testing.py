import logging, time
from pprint import pprint
from importlib import reload
from datetime import timedelta
import pandas as pd
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


df = gsigstr(mute=True,store=False)

for n in range(0,len(df["1h_sigresult"])):
    _print(df["1h_sigresult"][n])
for n in range(0,len(df["5m_sigresult"])):
    _print(df["5m_sigresult"][n])

print("SIGNAL SUMMARY")
print(df["df"])
print("")
print(df["5m_max"])
print(df["1h_max"])
print("")
