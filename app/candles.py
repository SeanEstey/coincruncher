# app.candles
import dateparser, logging, json, time, pytz
import textwrap
from pprint import pformat, pprint
from datetime import datetime
import pandas as pd
from pymongo import ReplaceOne
from binance.client import Client
from app import get_db
from app.timer import Timer
from app.utils import to_float, to_dt
log = logging.getLogger('candles')

#------------------------------------------------------------------------------
def get_new(pair):
    c = get_db().candles_5t.find({"pair":pair}).sort("date",-1).limit(1)
    return list(c)[0]

#------------------------------------------------------------------------------
def get_historic(pair, start, end):
    cursor = get_db().candles_5t.find(
        {"pair":pair, "date":{"$gte":start, "$lt":end}}
    ).sort("date",-1)
    df = pd.DataFrame(list(cursor))
    df.index = df["date"]
    del df["_id"]
    del df["date"]
    df_sum = df[["buy_vol", "trades", "volume"]].resample("1H").mean()
    print(df_sum.tail())
    df_mean = df[["close","high","low","open"]].resample("1H").mean()
    print(df_mean.tail())

    df_result = df_sum.join(df_mean)
    #df_hourly = df.resample("1H").mean().round(4).dropna()
    return df_result

#------------------------------------------------------------------------------
def query(pair, interval, start_str, end_str=None):
    """Get Historical Klines (candles) from Binance.
    @interval: Binance Kline interval
        i.e Client.KLINE_INTERVAL_30MINUTE, Client.KLINE_INTERVAL_1WEEK
    Return: list of OHLCV value
    """
    limit = 500
    idx = 0
    results = []
    timeframe = intrvl_to_ms(interval)
    start_ts = date_to_ms(start_str)
    end_ts = date_to_ms(end_str) if end_str else None
    client = Client("", "")

    while True:
        data = client.get_klines(symbol=pair, interval=interval, limit=limit,
            startTime=start_ts, endTime=end_ts)
        if len(data) > 0:
            results += data
            start_ts = data[len(data) - 1][0] + timeframe
        else:
            start_ts += timeframe
        idx += 1

        # Test limits/prevent API spamming
        if len(data) < limit:
            break
        if idx % 3 == 0:
            time.sleep(1)

    log.debug("%s %s candles retrieved (binance api)",
        len(results[0]) if len(results) > 0 else 0, pair.lower())
    return results

#------------------------------------------------------------------------------
def store(candles_df):
    tmr = Timer()
    pair = candles_df.ix[0]["pair"]
    bulk=[]
    for index, row in candles_df.iterrows():
        record = row.to_dict()
        record.update({"date":index.to_pydatetime()})
        bulk.append(
            ReplaceOne({"date":index.to_pydatetime(), "pair":pair}, record,
                upsert=True))

    if len(bulk) < 1:
        return log.info("No candles to store in DB")

    result = (get_db().candles_5t.bulk_write(bulk)
        ).bulk_api_result
    del result["upserted"], result["writeErrors"], result["writeConcernErrors"]
    log.debug("bulk_write completed (%sms)", tmr)
    log.debug(result)
    return result

#------------------------------------------------------------------------------
def to_df(pair, rawdata):
    """Convert candle data to pandas DataFrame.
    Unused:
       idx_6: close timestamp in ms (str)
       idx_7: total (quote) volume (str)
       idx_10: taker sell vol (quote pair)
       idx_11: (ignore)
    """
    df = pd.DataFrame(
        index=[to_dt(x[0]/1000) for x in rawdata], # idx_0: open timestamp in ms (str)
        data=[[
            pair,
            to_float(x[1],4),   # idx_1: open (str)
            to_float(x[2],4),   # idx_2: high (str)
            to_float(x[3],4),   # idx_3: low (str)
            to_float(x[4],4),   # idx_4: close (str)
            x[8],               # idx_8: num trades (int)
            to_float(x[9],4),   # idx_9: buy volume (base)
            to_float(x[5],4)    # idx_5: total volume (base)

        ] for x in rawdata],
        columns=["pair", "open", "high", "low", "close", "trades", "buy_vol", "volume"]
    )
    df.index.name = "date"
    return df

#------------------------------------------------------------------------------
def date_to_ms(date_str):
    """Convert UTC date to milliseconds
    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours
    ago UTC"
    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/
    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours
    ago UTC", "now UTC"
    :type date_str: str
    """
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    d = dateparser.parse(date_str)
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)
    return int((d - epoch).total_seconds() * 1000.0)

#------------------------------------------------------------------------------
def intrvl_to_ms(interval):
    """Convert a Binance interval string to milliseconds
    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h,
    6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str
    :return:
         None if unit not one of m, h, d or w
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }
    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            pass
    return ms
