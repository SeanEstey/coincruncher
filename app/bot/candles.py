# app.bot.candles
import logging
import time
from dateparser import parse
import pandas as pd
import numpy as np
from pymongo import ReplaceOne
from pymongo.errors import OperationFailure
from bsonnumpy import sequence_to_ndarray
from docs.conf import *
from docs.botconf import *
import app, app.bot
from app.common.timer import Timer
from app.common.utils import strtodt, strtoms
from app.common.timeutils import strtofreq

log = logging.getLogger('candles')

#------------------------------------------------------------------------------
def bulk_load(pairs, freqstrs, startstr=None, dfm=None):
    """Merge only newly updated DB records into dataframe to avoid ~150k
    DB reads every main loop.
    """
    db = app.get_db()
    t1 = Timer()
    columns = ['open', 'close', 'high', 'low', 'trades', 'volume', 'buy_vol']
    exclude = ['_id', 'quote_vol','sell_vol', 'close_time']
    proj = dict(zip(exclude, [False]*len(exclude)))
    query = {
        'pair': {'$in':pairs},
        'freqstr': {'$in':freqstrs}
    }

    if startstr:
        query['open_time'] = {'$gte':parse(startstr)}

    batches = db.candles.find_raw_batches(query, proj)
    if batches.count() < 1:
        print("No db matches for query {}.".format(query))
        return dfm

    dtype = np.dtype([
        ('pair', 'S12'),
        ('freqstr', 'S3'),
        ('open_time', np.int64),
        ('open', np.float64),
        ('close', np.float64),
        ('high', np.float64),
        ('low', np.float64),
        ('buy_vol', np.float64),
        ('volume', np.float64),
        ('trades', np.int32)
    ])
    # Bulk load mongodb records into predefined, fixed-size numpy array.
    # 10x faster than manually casting mongo cursor into python list.
    try:
        ndarray = sequence_to_ndarray(batches, dtype, batches.count())
    except Exception as e:
        print(str(e))
        return dfm

    # Build multi-index dataframe from ndarray
    df = pd.DataFrame(ndarray)
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['freqstr'] = df['freqstr'].str.decode('utf-8')
    df['pair'] = df['pair'].str.decode('utf-8')
    # Convert freqstr->freq to enable index sorting
    df = df.rename(columns={'freqstr':'freq'})
    [df['freq'].replace(n, strtofreq(n), inplace=True) for n in TRADEFREQS]
    df.sort_values(by=['pair','freq','open_time'], inplace=True)

    dfc = pd.DataFrame(df[columns].values,
        index = pd.MultiIndex.from_arrays(
            [df['pair'], df['freq'], df['open_time']],
            names = ['pair','freq','open_time']),
        columns = columns
    ).sort_index()

    # Merge if dataframe passed in
    n_bulk, n_merged = len(dfc), 0
    if dfm is not None:
        dfc = pd.concat([dfm, dfc]).drop_duplicates().sort_index()
        n_merged = len(dfc) - n_bulk
    log.debug("{:,} docs loaded, {:,} merged in {:,.1f} ms.".format(
        n_bulk, n_merged, t1))
    return dfc

#------------------------------------------------------------------------------
def bulk_save(data):
    """db.candles collection has unique index on (pair, freq, open_time) key
    so we can bulk insert without having to check for duplicates. Just need
    to set ordered=False and catch the exception, but every item insert
    will be attempted. May still be slow performance-wise...
    """
    t1 = Timer()
    n_insert = None
    try:
        result = app.get_db().candles.insert_many(data, ordered=False)
    except OperationFailure as e:
        n_insert = len(data) - len(e.details['writeErrors'])
        #print(e.details['writeErrors'][0])
    else:
        n_insert = len(result.inserted_ids)

    print("Saved {}/{} new records. [{} ms]".format(n_insert, len(data), t1))

#------------------------------------------------------------------------------
def update(pairs, freqstrs, startstr=None):
    db = app.get_db()
    client = app.bot.client
    t1 = Timer()
    candles = []

    # ##########################################################################
    # TODO Load all the data into dataframe first, reindex, and identify
    # missing data within timespan. Call query_api from first missing datapoint
    # rather than requesting all the data every time."""
    # ##########################################################################

    for pair in pairs:
        for freqstr in freqstrs:
            data = query_api(pair, freqstr, startstr=startstr)
            if len(data) == 0:
                continue
            for i in range(0, len(data)):
                x = data[i]
                x = [
                    pd.to_datetime(int(x[0]), unit='ms', utc=True),
                    float(x[1]),
                    float(x[2]),
                    float(x[3]),
                    float(x[4]),
                    float(x[5]),
                    pd.to_datetime(int(x[6]), unit='ms', utc=True),
                    float(x[7]),
                    int(x[8]),
                    float(x[9]),
                    float(x[10]),
                    None
                ]
                d = dict(zip(BINANCE_REST_KLINES, x))
                d.update({'pair': pair, 'freqstr': freqstr})
                data[i] = d
            candles += data

    bulk_save(candles)
    return candles

#------------------------------------------------------------------------------
def query_api(pair, freqstr, startstr=None, endstr=None):
    """Get Historical Klines (candles) from Binance.
    @freqstr: 1m, 3m, 5m, 15m, 30m, 1h, ettc
    """
    client = app.bot.client
    t1 = Timer()
    ms_period = strtofreq(freqstr) * 1000
    end = strtoms(endstr or "now utc") # if endstr else time.time()
    start = strtoms(startstr or DEF_KLINE_HIST_LEN)
    results = []

    #print("query_api start={}, end={}".format(start, end))

    while start < end:
        try:
            data = client.get_klines(
                symbol=pair,
                interval=freqstr,
                limit=BINANCE_REST_QUERY_LIMIT,
                startTime=start, endTime=end)
        except Exception as e:
            log.exception("Binance API request error. e=%s", str(e))
            continue

        if len(data) == 0:
            start += ms_period
        else:
            # Don't want candles that aren't closed yet
            #if data[-1][6] >= strtoms("now utc"):
            #    results += data[:-1]
            #    break
            results += data
            start = data[-1][0] + ms_period

    log.debug('%s %s %s queried [%ss].', len(results), freqstr, pair,
        t1.elapsed(unit='s'))
    return results

#------------------------------------------------------------------------------
def to_dict(pair, freqstr):
    """Get most recently added candle to either dataframe or mongoDB.
    """
    freq = strtofreq(freqstr)

    if (pair,freq) not in app.bot.dfc.index:
        raise Exception("({},{}) not in app.bot.dfc.index!".format(pair,freq))

    df = app.bot.dfc.loc[pair, freq].tail(1)

    if len(df) < 1:
        raise Exception("{},{},{} candle not found in app.bot.dfc".format(
            pair, freq, partial))

    return {
        **{'pair':pair, 'freqstr':freqstr, 'open_time':df.index.to_pydatetime()[0]},
        **df.to_dict('record')[0]
    }
