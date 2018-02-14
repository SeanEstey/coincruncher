# For testing in python interpreter
# Establish connection to remote DB
from importlib import reload
from pprint import pprint
import pandas as pd
import logging
from app import get_db, set_db
from app import markets, cryptocompare, analyze, tickers, coinmktcap, utils
from app.timer import Timer
log = logging.getLogger("testing")

pd.set_option("display.max_columns", 10)
pd.set_option("display.width", 1000)
hosts = ["localhost", "45.79.176.125"]
t1 = Timer()
set_db(hosts[0])
log.debug("Set db in %s ms", t1.clock(t='ms'))
db = get_db()
