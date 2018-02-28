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
from app.signals import multisigstr, sigstr
from pprint import pprint

#df = to_df("NEOBTC","1h", api_get("NEOBTC","1h","14 days ago UTC"), store_db=True)
