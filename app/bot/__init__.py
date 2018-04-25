# app.bot
import threading
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
# Pair change event
e_pairs = None
lock = threading.Lock()

#------------------------------------------------------------------------------
def init(evnt_pairs):
    from app.common.timer import Timer
    from app.common.timeutils import strtofreq
    from . import candles, scanner
    global client, dfc, e_pairs

    e_pairs = evnt_pairs
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
    #print("{} active pairs retrieved from api.".format(len(ops)))

    set_pairs([], 'DISABLED', query_temp=True)

    #print("{:,} historic candles loaded.".format(len(dfc)))
    print('{} trading algorithms.'.format(len(TRD_ALGOS)))
    print('app.bot initialized in {:,.0f} ms.'.format(t1.elapsed()))

#------------------------------------------------------------------------------
def get_pairs(with_temp=False):
    query = {'botTradeStatus':{'$in':['ENABLED']}}

    if with_temp is True:
        query['botTradeStatus']['$in'].append('TEMP')

    return set(n['symbol'] for n in list(app.db.assets.find(query)))

#------------------------------------------------------------------------------
def set_pairs(pairs, mode, exclusively=False, query_temp=False):
    """Set DB permissions for enabling/disabling trading of given pairs.
    """
    db = app.db
    enabled, disabled, inverse, trading, ops = [],[],[],[],[]

    pairs = set(pairs)
    positions = set(n['pair'] for n in db.trades.find({'status':'open'}))

    if mode == 'ENABLED':
        enabled = pairs - positions
        ops += [UpdateOne({'symbol':n}, {'$set':{'botTradeStatus':'ENABLED'}})\
            for n in enabled]
    elif mode == 'DISABLED':
        disabled = pairs - positions
        ops += [UpdateOne({'symbol':n}, {'$set':{'botTradeStatus':'DISABLED'}})\
            for n in disabled]

    if exclusively is True:
        all_ = set(n['symbol'] for n in db.assets.find({'status':'TRADING'}))
        inverse = all_ - pairs - positions
        ops += [UpdateOne({'symbol':n}, {'$set':{'botTradeStatus':'DISABLED'}})\
            for n in inverse]

    ops += [UpdateOne({'symbol':n}, {'$set':{'botTradeStatus':'TEMP'}})\
        for n in positions]
    ops = [n for n in ops if n is not None]

    lock.acquire()
    print("{} pair(s) enabled, {} disabled, {} temp."\
        .format(len(enabled), len(inverse) + len(disabled), len(positions)))
    #print("Querying candles for {} new pairs..."\
    #    .format(len(enabled)+len(positions)))
    lock.release()

    querylist = list(enabled)
    if query_temp is True:
        querylist += list(positions)

    # Query historic candle data + load.
    for pair in querylist:
        lock.acquire()
        print("Retrieving {} candles...".format(pair))
        lock.release()
        candles.bulk_append_dfc(candles.api_update([pair], TRD_FREQS, silent=True))

    # Update DB and alert websock thread to update sockets.
    if len(ops) > 0:
        result = db.assets.bulk_write(ops)
        #print("Updated {} DB permissions.".format(len(ops)))

        e_pairs.set()

    return (enabled,disabled,inverse,positions)
