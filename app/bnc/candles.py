import logging
import pandas as pd
from pymongo import UpdateOne
from binance.client import Client
import app
from app import strtofreq
from docs.data import BINANCE
from app.timer import Timer
from app.utils import intrvl_to_ms, datestr_to_dt, datestr_to_ms, dt_to_ms, utc_datetime as now
from app.mongo import locked
log = logging.getLogger('candles')

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
            _dict = dict(zip(BINANCE['KLINE_FIELDS'], x))
            _dict.update({'pair': pair, 'freq': freq})
            if _dict['volume'] > 0:
                _dict['buy_ratio'] = round(_dict['buy_vol'] / _dict['volume'], 4)
            else:
                _dict['buy_ratio'] = 0.0
            data[i] = _dict
        candles += data

    if len(candles) > 0:
        db = app.get_db()

        if force == True:
            ops = []
            for candle in candles:
                ops.append(UpdateOne(
                    {"open_time":candle["open_time"], "pair":candle["pair"], "freq":candle["freq"]},
                    {'$set':candle},
                    upsert=True
                ))
            result = db.candles.bulk_write(ops)
        else:
            # Should not create any duplicates because of force==False
            # check in query_api()
            result = db.candles.insert_many(candles)

    log.info("%s %s records queried/stored. [%ss]",
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
    columns = ['close', 'open', 'trades', 'volume', 'buy_ratio']
    t1 = Timer()
    idx, data = [], []
    time_span = time_span if time_span else timedelta(days=21)

    curs = app.get_db().candles.find(
        {"pair":{"$in":pairs}, "close_time":{"$gte":now() - time_span}})

    for candle in curs:
        idx.append((candle['pair'], strtofreq[candle['freq']], candle['open_time']))
        data.append( [candle.get(x, None) for x in columns] )

    df_new = pd.DataFrame(data,
        index = pd.Index(idx, names=['pair', 'freq', 'open_time']),
        columns = columns)

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
            dt = list(newer)[0]['open_time']
            start_ts = int(dt.timestamp()*1000 + periodlen)

            if start_ts > end_ts:
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

    log.debug('%s %s %s queried [%ss].', len(results), freq, pair, t1.elapsed(unit='s'))
    return results
