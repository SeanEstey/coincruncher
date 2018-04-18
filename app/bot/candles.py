# app.bot.candles
import logging
import time
from dateparser import parse
import pandas as pd
import numpy as np
from pymongo import ReplaceOne
from bsonnumpy import sequence_to_ndarray
from docs.conf import *
from docs.botconf import *
import app, app.bot
from app.common.timer import Timer
from app.common.utils import strtodt, strtoms
from app.common.timeutils import strtofreq

log = logging.getLogger('candles')

#------------------------------------------------------------------------------
def load(pairs, freqstrs, startstr=None, dfm=None):
    """Merge only newly updated DB records into dataframe to avoid ~150k
    DB reads every main loop.
    """
    db = app.get_db()
    t1 = Timer()
    columns = ['open', 'close', 'high', 'low', 'trades', 'volume', 'buy_ratio']
    exclude = ['_id', 'quote_vol','sell_vol', 'close_time']
    proj = dict(zip(exclude, [False]*len(exclude)))
    query = {
        'pair': {'$in':pairs},
        'freq': {'$in':freqstrs}
    }

    if startstr:
        query['open_time'] = {'$gte':parse(startstr)}

    batches = db.candles.find_raw_batches(query, proj)
    if batches.count() < 1:
        print("No db matches for ({},{}).".format(query))
        return dfm

    dtype = np.dtype([
        ('pair', 'S12'),
        ('freq', 'S3'),
        ('open_time', np.int64),
        ('open', np.float64),
        ('close', np.float64),
        ('high', np.float64),
        ('low', np.float64),
        ('buy_vol', np.float64),
        ('volume', np.float64),
        ('buy_ratio', np.float64),
        ('trades', np.int32)
    ])
    # Bulk load mongodb records into predefined, fixed-size numpy array.
    # 10x faster than manually casting mongo cursor into python list.
    try:
        ndarray = sequence_to_ndarray(batches, dtype, batches.count())
    except Exception as e:
        log.error(str(e))
        return dfm

    # Build multi-index dataframe from ndarray
    df = pd.DataFrame(ndarray)
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['freq'] = df['freq'].str.decode('utf-8')
    df['pair'] = df['pair'].str.decode('utf-8')
    # Convert freqstr->freq to enable index sorting
    [df['freq'].replace(n, strtofreq(n), inplace=True) \
        for n in df['freq'].drop_duplicates()]

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
def update(pairs, freqstrs, startstr=None):
    db = app.get_db()
    client = app.bot.client
    t1 = Timer()
    candles = []

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
                d.update({'pair': pair, 'freq': freqstr})
                if d['volume'] > 0:
                    d['buy_ratio'] = round(d['buy_vol'] / d['volume'], 4)
                else:
                    d['buy_ratio'] = 0.0
                data[i] = d
            candles += data

    # Setting ordered to False will attempt all inserts even one or more
    # errors occur due to attempting to insert duplicate unique index keys.
    # If ordered is True, a single error will abort all remaining inserts.
    if len(candles) > 0:
        try:
            result = db.candles.insert_many(candles, ordered=False)
        except Exception as e:
            print("MongoDB bulk insert error. Msg: {}".format(str(e)))
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
            if data[-1][6] >= time.time():
                results += data[:-1]
                break
            results += data
            start = data[-1][0] + ms_period

    log.debug('%s %s %s queried [%ss].', len(results), freqstr, pair,
        t1.elapsed(unit='s'))
    return results

#------------------------------------------------------------------------------
def to_dict(pair, freqstr, partial=False):
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
        **{'pair':pair, 'freq':freqstr, 'open_time':df.index.to_pydatetime()[0]},
        **df.to_dict('record')[0]
    }

#------------------------------------------------------------------------------
def describe(candle):
    from app.bot.trade import snapshot
    ss = snapshot(candle)

    line = "{:<7} {:>5} {:>+10.2f} z-p {:>+10.2f} z-v {:>10.2f} bv"\
           "{:>+10.2f} m" #{:>+10.2f} macd{}{}"

    siglog(line.format(candle['pair'], candle['freq'],
        ss['price']['z-score'], ss['volume']['z-score'], candle['buy_ratio'],
        ss['price']['emaDiff']
    ))
