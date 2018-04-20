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

    pairs = enable_pairs(scanner.filter_pairs())
    dfc = candles.bulk_load(pairs, TRADEFREQS, dfm=dfc)

    print("{:,} historic candles loaded.".format(len(dfc)))
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
    # Add open trade pairs
    tradepairs = set([n['pair'] for n in list(db.trades.find({'status':'open'}))])
    pairs = list(set(pairs + list(tradepairs)))

    # Retrieve historic data and load.
    for pair in pairs:
        print("Retrieving {} candles...".format(pair))
        candles.update([pair], TRADEFREQS)

    result = db.assets.update_many({},
        {'$set':{'botTradeStatus':'DISABLED', 'botTradeFreq':[]}})

    ops = [
        UpdateOne({'symbol':pair},
            {'$set':{'botTradeStatus':'ENABLED'},
            '$push':{'botTradeFreq':TRADEFREQS}},
            upsert=True) for pair in pairs
    ]

    if len(ops) > 0:
        result = db.assets.bulk_write(ops)
        n_enabled = db.assets.find({'botTradeStatus':'ENABLED'}).count()
        print("{} pairs enabled:".format(n_enabled))
        print(pairs)
    return pairs


