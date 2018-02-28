import logging, time
from importlib import reload
from datetime import timedelta as tdelta
import pandas as pd
from app import get_db, set_db
from app.timer import Timer

# Config
log = logging.getLogger("testing")
pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
hosts = ["localhost", "45.79.176.125"]
set_db(hosts[0])
db = get_db()
t1 = Timer()

#------------------------------------------------------------------------------
from app.utils import utc_datetime, utc_dtdate
from app.candles import api_get
from app.signals import gsigstr, multisigstr, sigstr
from pprint import pprint

def refresh_api_data():
    from config import BINANCE_PAIRS
    for pair in BINANCE_PAIRS:
        api_get(pair, "5m", "3 hours ago UTC")
        time.sleep(3)
        api_get(pair, "1h", "72 hours ago UTC")
        time.sleep(1)

#df = to_df("NEOBTC","1h", api_get("NEOBTC","1h","14 days ago UTC"), store_db=True)
