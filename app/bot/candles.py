import logging
from dateparser import parse
import pandas as pd
import numpy as np
from pymongo import ReplaceOne
from bson import ObjectId
from bsonnumpy import sequence_to_ndarray
from binance.client import Client
import app
from docs.conf import binance as _conf
from app.common.timer import Timer
from app.common.timeutils import strtofreq
from app.common.utils import datestr_to_dt, datestr_to_ms, dt_to_ms, utc_datetime as now
from app.common.mongo import locked
import app.bot
log = logging.getLogger('candles')

#------------------------------------------------------------------------------
def load(pairs, freqstr=None, startstr=None, dfm=None):
    """Merge only newly updated DB records into dataframe to avoid ~150k
    DB reads every main loop.
    """
    t1 = Timer()
    columns = ['open', 'close', 'high', 'low', 'trades', 'volume', 'buy_ratio']
    exclude = ['_id', 'quote_vol','sell_vol', 'close_time']
    proj = dict(zip(exclude, [False]*len(exclude)))
    idx, data = [], []
    db = app.get_db()
    query = {'pair':{'$in':pairs}}
    if startstr:
        query['open_time'] = {'$gte':parse(startstr)}
    if freqstr:
        query['freq'] = freqstr

    # Bulk load mongodb records into predefined, fixed-size numpy array.
    # 10x faster than manually casting mongo cursor into python list.
    batches = db.candles.find_raw_batches(query, proj)
    if batches.count() < 1:
        print("No DB matches found for query: {}".format(query))
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
    df['partial'] = False
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
def update(pairs, freqstr, start=None, force=False):
    idx = 0
    t1 = Timer()
    candles = []

    for pair in pairs:
        data = query_api(pair, freqstr, start=start, force=force)
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
            d = dict(zip(_conf['kline_fields'], x))
            d.update({'pair': pair, 'freq': freqstr, 'partial':False})
            if d['volume'] > 0:
                d['buy_ratio'] = round(d['buy_vol'] / d['volume'], 4)
            else:
                d['buy_ratio'] = 0.0
            data[i] = d
        candles += data

    if len(candles) > 0:
        db = app.get_db()

        if force == True:
            ops = []
            for candle in candles:
                ops.append(ReplaceOne(
                    {"close_time":candle["close_time"],
                        "pair":candle["pair"], "freq":candle["freq"]},
                    candle,
                    upsert=True
                ))
            result = db.candles.bulk_write(ops)
        else:
            # Should not create any duplicates because of force==False
            # check in query_api()
            result = db.candles.insert_many(candles)

    log.info("%s %s records queried/stored. [%ss]",
        len(candles), freqstr, t1.elapsed(unit='s'))

    return candles

#------------------------------------------------------------------------------
def query_api(pair, freqstr, start=None, end=None, force=False):
    """Get Historical Klines (candles) from Binance.
    @freqstr: Binance kline frequency:
        1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M]
        m -> minutes; h -> hours; d -> days; w -> weeks; M -> months
    @force: if False, only query unstored data (faster). If True, query all.
    Return: list of OHLCV value
    """
    t1 = Timer()
    limit = 500
    idx = 0
    results = []
    periodlen = strtofreq(freqstr) * 1000
    end_ts = datestr_to_ms(end) if end else dt_to_ms(now())
    start_ts = datestr_to_ms(start) if start else end_ts - (periodlen * 20)

    # Skip queries for records already stored
    if force == False:
        query = {"pair":pair, "freq":freq}
        if start:
            query["open_time"] = {"$gt": datestr_to_dt(start)}

        newer = app.get_db().candles.find(query).sort('open_time',-1).limit(1)

        if newer.count() > 0:
            dt = list(newer)[0]['open_time']
            start_ts = int(dt.timestamp()*1000 + periodlen)

            if start_ts > end_ts:
                log.debug("All records for %s already stored.", pair)
                return []

    client = Client("", "")

    #while len(results) < 500 and start_ts < end_ts:
    while start_ts < end_ts:
        try:
            data = client.get_klines(symbol=pair, interval=freqstr,
                limit=limit, startTime=start_ts, endTime=end_ts)

            if len(data) == 0:
                start_ts += periodlen
            else:
                # Don't want candles that aren't closed yet
                if data[-1][6] >= dt_to_ms(now()):
                    results += data[:-1]
                    break
                results += data
                start_ts = data[-1][0] + periodlen
        except Exception as e:
            log.exception("Binance API request error. e=%s", str(e))

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
    #df = df[df['partial'] == partial].tail(1)

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
