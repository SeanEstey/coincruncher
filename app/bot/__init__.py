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

    enabled = db.assets.find({'botTradeStatus':'ENABLED'})

    if enabled.count() == 0:
        #raise Exception("No tradepairs enabled")
        print("No trade pairs enabled. Running scanner.")
        scanner.update()

    pairs = [ n['symbol'] for n in list(enabled) ]

    print("{} enabled pairs: {}".format(enabled.count(), pairs))

    # Load any available candle data for enabled pairs.
    dfc = candles.load(pairs, TRADEFREQS, dfm=pd.DataFrame())

    # Query the rest.
    tuples = pd.MultiIndex.from_product([pairs, TRADEFREQS]).values.tolist()
    for idx in tuples:
        if (idx[0], strtofreq(idx[1])) in dfc.index:
            continue

        print("Retrieving {} candles...".format(idx))

        candles.update([idx[0]], [idx[1]])
        dfc = candles.load([idx[0]], [idx[1]], dfm=dfc)

    print("{} historic candles loaded.".format(len(dfc)))
    print('{} trading algorithms.'.format(len(TRADE_ALGOS)))
    print('app.bot initialized in {:,.0f} ms.'.format(t1.elapsed()))

#------------------------------------------------------------------------------
def get_pairs():
    enabled = app.get_db().assets.find({'botTradeStatus':'ENABLED'})
    return [ n['symbol'] for n in list(enabled) ]

#------------------------------------------------------------------------------
def enable_pairs(pairs):
    """To disable all pairs/freq, pass in empty list.
    """
    db = app.db

    result = db.assets.update_many({}, {'$set':{'botTradeStatus':'DISABLED',
        'botTradeFreq':[]}})
    n_enabled = db.meta.find({'botTradeStatus':'ENABLED'}).count()

    print("Reset {}/{} pairs.".format(result.modified_count, n_enabled))

    ops = [
        UpdateOne({'symbol':pair},
            {'$set':{'botTradeStatus':'ENABLED'},
            '$push':{'botTradeFreq':TRADEFREQS}},
            upsert=True) for pair in pairs
    ]

    if len(ops) > 0:
        result = db.assets.bulk_write(ops)

        print("{} pairs updated, {} upserted.".format(
            result.modified_count, result.upserted_count))
