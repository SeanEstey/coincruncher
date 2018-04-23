# app.bot
from pymongo import UpdateOne
import pandas as pd
from binance.client import Client
from docs.botconf import *
import app

##### Globals ##################################################################

# Holds all historic candle data. Loaded once at app init, new data is merged
# in as needed.
dfc = pd.DataFrame()
# Binance client for all modules in app.bot. Initialized in init as
# singleton structure.
client = None

#------------------------------------------------------------------------------
def init():
    from app.common.timer import Timer
    from app.common.timeutils import strtofreq
    from . import candles, scanner
    global client, dfc

    t1 = Timer()
    db = app.get_db()

    # Auth Binance client.
    cred = list(db.api_keys.find())[0]
    client = Client(cred['key'], cred['secret'])

    # Get available exchange trade pairs
    info = client.get_exchange_info()
    ops = [ UpdateOne({'symbol':n['symbol']}, {'$set':n},
        upsert=True) for n in info['symbols'] ]
    db.assets.bulk_write(ops)

    print("{} active pairs retrieved from api.".format(len(ops)))
    print("Running scanner...")

    pairs = update_pairs(scanner.filter_pairs())

    print("{:,} historic candles loaded.".format(len(dfc)))
    print('{} trading algorithms.'.format(len(TRADE_ALGOS)))
    print('app.bot initialized in {:,.0f} ms.'.format(t1.elapsed()))

#------------------------------------------------------------------------------
def get_pairs():
    enabled = app.get_db().assets.find({'botTradeStatus':'ENABLED'})
    return [ n['symbol'] for n in list(enabled) ]

#------------------------------------------------------------------------------
def update_pairs(pairs, query_all=True):
    """Update authorized trading pair list, query historic data and load into
    global dataframe.
    @query_all: if set to False, will only query historic data for new
    pairs not already loaded into global candle dataframe.
    """
    db = app.db
    querylist = []
    # Add open trade pairs
    tradepairs = set([n['pair'] for n in list(db.trades.find({'status':'open'}))])

    if len(set(pairs + list(tradepairs))) == 0:
        raise Exception("No pairs enabled for trading.")

    if query_all == True:
        querylist = list(set(pairs + list(tradepairs)))
        print("Querying historic data for all {} pairs...".format(len(querylist)))
    else:
        querylist = list(set(pairs) - set(get_pairs()))
        print("Querying historic data for {} new pairs...".format(len(querylist)))

    if len(querylist) > 0:
        # Retrieve historic data and load.
        candlelist = []
        for pair in querylist:
            print("Retrieving {} candles...".format(pair))
            candlelist += candles.update([pair], TRADEFREQS)
        candles.bulk_append_dfc(candlelist)

    result = db.assets.update_many({},
        {'$set':{'botTradeStatus':'DISABLED', 'botTradeFreq':[]}})

    enabled = list(set(pairs + list(tradepairs)))
    ops = [
        UpdateOne({'symbol':pair},
            {'$set':{'botTradeStatus':'ENABLED'},
            '$push':{'botTradeFreq':TRADEFREQS}},
            upsert=True) for pair in enabled
    ]

    if len(ops) > 0:
        result = db.assets.bulk_write(ops)
        n_enabled = db.assets.find({'botTradeStatus':'ENABLED'}).count()
        print("{} pairs enabled:".format(n_enabled))
        print(pairs)

    return enabled
