import logging, time
from time import sleep
import pandas as pd
import numpy as np
from pymongo import UpdateOne, ReplaceOne
from binance.client import Client
import app
from app import freqtostr, strtofreq
from app.timer import Timer
from app.utils import intrvl_to_ms, datestr_to_dt, datestr_to_ms, dt_to_ms
from app.utils import utc_datetime as now
from app.mongo import locked
log = logging.getLogger('candles')

binance_kline=[
    'open_time',
    'open',
    'high',
    'low',
    'close',
    'volume',
    'close_time',
    'quote_vol',        # Total quote asset vol
    'trades',
    'buy_vol',          # Taker buy base asset vol
    'buy_quote_vol',    # Taker buy quote asset vol
    'ignore'
]

#------------------------------------------------------------------------------
def update(pairs, freq, start=None, force=False):
    idx = 0
    t1 = Timer()
    candles = []

    for pair in pairs:
        data = query_api(pair, freq, start=start, force=force)

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
            _dict = dict(zip(binance_kline, x))
            _dict.update({'pair': pair, 'freq': freq})
            if _dict['volume'] > 0:
                _dict['buy_ratio'] = _dict['buy_vol'] / _dict['volume']
            else:
                _dict['buy_ratio'] = 0.0
            data[i] = _dict
        candles += data

    if len(candles) > 0:
        db = app.get_db()

        if force == True:
            # TODO: DONT OVERWRITE 'ZSCORE' DATA!!!
            # Upsert one by one to avoid duplicates. Slow.
            ops = []
            for candle in candles:
                ops.append(UpdateOne(
                    {"open_time":candle["open_time"], "pair":candle["pair"], "freq":candle["freq"]},
                    {'$set':candle},
                    upsert=True
                ))
            db.candles.bulk_write(ops)
        else:
            db.candles.insert_many(candles)

    #print("%s %s candles updated." % (len(candles), freq))
    log.info("%s %s candle records updated. [%ss]",
        len(candles), freq, t1.elapsed(unit='s'))

    return candles

#------------------------------------------------------------------------------
def newest(pair, freq_str, df=None):
    """Get most recently added candle to either dataframe or mongoDB.
    """
    freq = strtofreq[freq_str]

    if df is not None:
        series = df.loc[pair, freq].iloc[-1]
        open_time = df.loc[(pair,freq)].index[-1]
        idx = dict(zip(df.index.names, [pair, freq_str, open_time]))
        return {**idx, **series}
    else:
        log.debug("Doing DB read for candle.newest!")

        db = app.get_db()
        return list(db.candles.find({"pair":pair,"freq":freq})\
            .sort("close_time",-1)\
            .limit(1)
        )[0]

#------------------------------------------------------------------------------
def merge(df, pairs, time_span=None):
    """Merge only newly updated DB records into dataframe to avoid ~150k DB reads
    every main loop.
    """
    from docs.config import Z_FACTORS
    t1 = Timer()
    idx, data = [], []
    time_span = time_span if time_span else timedelta(days=21)

    curs = app.get_db().candles.find(
        {"pair":{"$in":pairs}, "close_time":{"$gte":now() - time_span}})

    for candle in curs:
        idx.append((candle['pair'], strtofreq[candle['freq']], candle['open_time']))
        data.append([candle[x.lower()] for x in Z_FACTORS])

    df_new = pd.DataFrame(data,
        index = pd.Index(idx, names=['PAIR', 'FREQ', 'OPEN_TIME']),
        columns = Z_FACTORS)

    df = pd.concat([df, df_new]).drop_duplicates().sort_index()

    log.debug("{:,} candle records merged. [{:,.1f} ms]".format(len(df), t1))

    return df

#------------------------------------------------------------------------------
def query_api(pair, freq, start=None, end=None, force=False):
    """Get Historical Klines (candles) from Binance.
    https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md
    @freq: Binance kline frequency:
        1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M]
        m -> minutes; h -> hours; d -> days; w -> weeks; M -> months
    @force: if False, only query unstored data (faster). If True, query all.
    Return: list of OHLCV value
    """
    t1 = Timer()
    limit = 500
    idx = 0
    results = []
    periodlen = intrvl_to_ms(freq)
    end_ts = datestr_to_ms(end) if end else dt_to_ms(now())
    start_ts = datestr_to_ms(start) if start else end_ts - (periodlen * 20)

    # Skip queries for records already stored
    if force == False:
        query = {"pair":pair, "freq":freq}
        if start:
            query["open_time"] = {"$gt": datestr_to_dt(start)}

        newer = app.get_db().candles.find(query).sort('open_time',-1).limit(1)

        if newer.count() > 0:
            print("skipping %s already stored" % newer.count())

            dt = list(newer)[0]['open_time']
            start_ts = int(dt.timestamp()*1000 + periodlen)

            if start_ts > end_ts:
                print("All records for %s already stored" % pair)
                log.debug("All records for %s already stored.", pair)
                return []

    client = Client("", "")

    #while len(results) < 500 and start_ts < end_ts:
    while start_ts < end_ts:
        try:
            data = client.get_klines(symbol=pair, interval=freq,
                limit=limit, startTime=start_ts, endTime=end_ts)

            if len(data) == 0:
                start_ts += periodlen
            else:
                # Don't want candles that aren't closed yet
                if data[-1][6] >= dt_to_ms(now()):
                    #print("discarding unclosed candle w/ close_time %s" %(
                    #    pd.to_datetime(int(data[-1][6]), unit='ms', utc=True)))
                    results += data[:-1]
                    break
                results += data
                start_ts = data[-1][0] + periodlen
        except Exception as e:
            log.exception("Binance API request error. e=%s", str(e))

    log.debug('%s %s %s results. [%ss].', len(results), freq, pair, t1.elapsed(unit='s'))
    #print("%s result(s)" % len(results))
    return results

#------------------------------------------------------------------------------
def save_db(candles, freq, df):
    """
    """
    tmr = Timer()
    bulk=[]
    db = app.get_db()

    for idx, row in df.iterrows():
        record = row.to_dict()
        bulk.append(ReplaceOne(
            {"pair":pair, "freq":freq, "open_time":record["open_time"]},
            record,
            upsert=True))

    if len(bulk) < 1:
        return log.debug("No candle data")

    try:
        result = app.get_db().candles.bulk_write(bulk)
    except Exception as e:
        return log.exception("mongodb write error. locked=%s, result=%s", locked(), result)

    del result["upserted"], result["writeErrors"], result["writeConcernErrors"]
    #log.debug("stored to db (%sms)", tmr)
    return result

#------------------------------------------------------------------------------
def load_db(pair, freq, start=None, end=None):
    """Returns:
        pd.DataFrame w/ OHLC candle data
        df.index: open_time
    """
    t1 = Timer()
    cols = ["pair", "freq", "close_time", "trades", "open", "high", "low", "close",
        "buy_vol", "buy_ratio", "volume"]

    match = {"pair":pair, "freq":freq}
    match["open_time"] = {"$lt": end if end else now()}

    if start is not None:
        match["open_time"]["$gte"] = start

    data = list(app.get_db().candles.find(match)) #.sort("open_time",-1))
    index = pd.Index([pd.to_datetime(n["open_time"], utc=True) for n in data],
        name="open_time")
    df = pd.DataFrame(data, index=index, columns=cols)

    df[["trades"]] = df[["trades"]].astype('int32')
    df[cols[4:]] = df[cols[4:]].astype('float64')
    log.debug('loaded %s candle records into df. [%s]', len(df), t1)
    return df

#------------------------------------------------------------------------------
def to_df(pair, freq, rawdata):
    """Convert candle data to pandas DataFrame.
    https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md
    BTCUSDT:
        BTC: base asset
        USDT: quote asset
    @freq: Binance interval (NOT pandas frequency format!)
    """
    df = pd.DataFrame(
        index = [pd.to_datetime(x[0], unit='ms', utc=True) for x in rawdata],
        data = [x for x in rawdata],
        columns = binance_klines
    )
    is_npfloat64 = ['open','high','low','close','buy_vol','volume']
    df[is_npfloat64] = df[is_npfloat64].astype(np.float64)
    df.index.name="date"
    del df["ignore"]
    df["pair"] = pair
    df["freq"] = freq
    df["close_time"] = pd.to_datetime(df["close_time"],unit='ms',utc=True)
    df["open_time"] = pd.to_datetime(df["open_time"],unit='ms',utc=True)
    df["buy_ratio"] = df["buy_vol"] / df["volume"]
    df = df[sorted(df.columns)]
    return df
