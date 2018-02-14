# For testing in python interpreter
# Establish connection to remote DB
from importlib import reload
from pprint import pprint
import pandas as pd
import logging
from app import get_db, set_db
from app import markets, tickers, coinmktcap, utils
from app.timer import Timer
log = logging.getLogger("testing")

def top_symbols(rank):
    """Get list of ticker symbols within given rank.
    """
    global db
    cursor = db.tickers_1d.aggregate([
        {"$match":{"rank_now":{"$lte":rank}}},
        {"$group":
              {"_id":"$symbol", "rank":{"$last":"$rank_now"}, "date":{"$max":"$date"}}
        },
        {"$sort":{"rank":1}}
    ])
    return [n["_id"] for n in list(cursor)]

pd.set_option("display.max_columns", 10)
pd.set_option("display.width", 1000)
hosts = ["localhost", "45.79.176.125"]
t1 = Timer()
set_db(hosts[0])
log.debug("Set db in %s ms", t1.clock(t='ms'))
db = get_db()
