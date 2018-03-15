# app.candles
import logging, time
from time import sleep
import pandas as pd
import numpy as np
from decimal import Decimal
from pymongo import ReplaceOne
from binance.client import Client
import app
from app.timer import Timer
from app.utils import intrvl_to_ms, date_to_ms, dt_to_ms
from app.utils import utc_datetime as now
from app.mongo import locked
log = logging.getLogger('candles')

#------------------------------------------------------------------------------
def last(pair, freq):
    return list(app.get_db().candles.find({"pair":pair,"freq":freq}
        ).sort("open_time",-1).limit(1))[0]

#------------------------------------------------------------------------------
def load_db(pair, freq, start=None, end=None):
    """Return historical average candle data from DB as dataframe.
    """
    t1 = Timer()
    cols = ["pair", "freq", "close_time", "trades", "open", "high", "low", "close",
        "buy_vol", "buy_ratio", "volume"]

    match = {"pair":pair, "freq":freq}
    match["open_time"] = {"$lt": end if end else now()}

    if start is not None:
        match["open_time"]["$gte"] = start

    data = list(app.get_db().candles.find(match).sort("open_time",1))

    df = pd.DataFrame(
        data,
        index = pd.Index([pd.to_datetime(n["open_time"], utc=True) for n in data], name="open_time"),
        columns = cols
    )
    df[["trades"]] = df[["trades"]].astype('int32')
    df[cols[4:]] = df[cols[4:]].astype('float64')
    return df

#------------------------------------------------------------------------------
def save_db(pair, freq, candles_df):
    tmr = Timer()
    bulk=[]

    for idx, row in candles_df.iterrows():
        record = row.to_dict()
        bulk.append(ReplaceOne(
            {"pair":pair, "freq":freq, "open_time":record["open_time"]},
            record,
            upsert=True))

    if len(bulk) < 1:
        return log.debug("No candle data")

    try:
        result = (app.get_db().candles.bulk_write(bulk)).bulk_api_result
    except Exception as e:
        return log.exception("mongodb write error. locked=%s, result=%s", locked(), result)

    del result["upserted"], result["writeErrors"], result["writeConcernErrors"]
    #log.debug("stored to db (%sms)", tmr)
    return result

#------------------------------------------------------------------------------
def to_df(pair, freq, rawdata, store_db=False):
    """Convert candle data to pandas DataFrame.
    @freq: Binance interval (NOT pandas frequency format!)
    """
    _npfloats = ["open","high","low","close","buy_vol","volume"]

    columns=["open_time","open","high","low","close","volume","close_time",
        "quote_vol", "trades", "buy_vol", "sell_vol", "ignore"]

    df = pd.DataFrame(
        index = [pd.to_datetime(x[0], unit='ms', utc=True) for x in rawdata],
        data = [x for x in rawdata],
        columns = columns
    )
    df[_npfloats] = df[_npfloats].astype(np.float64)
    df.index.name="date"
    del df["ignore"]
    df["pair"] = pair
    df["freq"] = freq
    df["close_time"] = pd.to_datetime(df["close_time"],unit='ms',utc=True)
    df["open_time"] = pd.to_datetime(df["open_time"],unit='ms',utc=True)
    df["buy_ratio"] = df["buy_vol"] / df["volume"]
    df = df[sorted(df.columns)]

    if store_db:
        save_db(pair, freq, df)
    return df

#------------------------------------------------------------------------------
def api_get_all(pairs, freq, periodlen):
    idx = 0
    t1 = Timer()

    for pair in pairs:
        t2 = Timer()
        results = api_get(pair, freq, periodlen)
        log.debug("%s %s candles updated. [%ss]", freq, pair, t2.elapsed(unit='s'))
        idx += 1
        if idx % 3==0:
            sleep(1)
    log.info("%s %s binance candles updated. [%ss]", len(pairs), freq, t1.elapsed(unit='s'))

#------------------------------------------------------------------------------
def api_get(pair, interval, start_str, end_str=None, store_db=True):
    """Get Historical Klines (candles) from Binance.
    @interval: Binance kline values: [1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h,
    12h, 1d, 3d, 1w, 1M]
    Return: list of OHLCV value
    """
    limit = 500
    idx = 0
    results = []
    periodlen = intrvl_to_ms(interval)
    start_ts = date_to_ms(start_str)
    end_ts = date_to_ms(end_str) if end_str else dt_to_ms(now())
    client = Client("", "")

    while len(results) < 500 and start_ts < end_ts:
        try:
            data = client.get_klines(
                symbol=pair,
                interval=interval,
                limit=limit,
                startTime=start_ts,
                endTime=end_ts)

            if len(data) == 0:
                start_ts += periodlen
            else:
                # close_time > now?
                if data[-1][6] >= dt_to_ms(now()):
                    results += data[:-1]
                    break
                results += data
                start_ts = data[len(data) - 1][0] + periodlen

            idx += 1
            if idx % 3==0:
                sleep(1)
        except Exception as e:
            return log.exception("Binance API request error. e=%s", str(e))

    #log.debug("api_get() result: %s loops, %s %s items", idx, len(results), pair)

    if store_db:
        save_db(pair, interval, to_df(pair, interval, results))

    return results
