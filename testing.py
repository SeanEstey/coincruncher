# For testing in python interpreter
# Establish connection to remote DB
from importlib import reload
import logging
from app import get_db, set_db
from app import markets, tickers, coinmktcap, utils
from app.timer import Timer
log = logging.getLogger("client")

t1 = Timer()
set_db("45.79.176.125")
log.debug("Set db in %s ms", t1.clock(t='ms'))
db = get_db()

