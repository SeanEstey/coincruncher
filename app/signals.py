""" app.signals
It’s simple supply and demand: a sudden increase in the number of buyers
over sellers in a short space of time. In this scenario, certain phenomena
can be observed such as:
    -An acceleration of volume within a short period of time.
    -An increase in price volatility over historic averages.
    -An increase in the ratio of volume traded on bid/ask versus normal.

Each of these phenomena can be quantified by tracking the following factors:
    -Volume traded over an hourly period.
    -Change in price over an hourly period.
    -Volume of the coin traded on the offer divided by the volume of the
    coin traded on the bid over an hourly period.

Every factor is stored by CoinFi (in this case hourly) and plotted on a
normal distribution. When the latest hourly data is updated, the model
reruns comparing the current factor value versus the historical factor
values. The data is plotted on a normal distribution and any current
factor value that is 2 standard deviations above the mean triggers an alert.

When there are multiple factors triggering alerts, they indicate more
confidence in the signal. Example:
    -Volume traded in the last hour was >= 3 STD over mean; AND
    -Change in price over the last hour is >=2 STD over mean; AND
    -Volume ratio of offer/bid is >= 2 STD over mean

The model will take in factors over multiple time periods (i.e. minute,
hourly, daily, weekly) and will also compare against multiple
historical time periods (i.e. volume over last 24 hours, 48 hours, 72 hours)

Furthermore CoinFi stores these factors across a selected universe of coins
and the model is continuously run, outputting alerts when a factor or a
series of factors has a positive signal.
"""

import logging
import sys
from pymongo import ReplaceOne, UpdateOne
from datetime import timedelta
import pandas as pd
import numpy as np
from app import get_db
from app.candles import db_get, last
from app.timer import Timer
from app.utils import utc_datetime as now
from docs.data import BINANCE
log = logging.getLogger('signals')

#------------------------------------------------------------------------------
def gsigstr(dbstore=False):
    """Get signal dataframe for all combined pairs.
    """
    _1m = timedelta(minutes=1)
    _1h = timedelta(hours=1)
    _1d = timedelta(hours=24)
    dfsig = pd.DataFrame()

    # Generate signal scores for each (Pair,Freq,Period) tuple.
    for pair in BINANCE["CANDLES"]:
        c5m = last(pair,"5m")
        t5m = [c5m["open_date"] - (5*_1m), c5m["close_date"] - (5*_1m)]
        c1h = last(pair,"1h")
        t1h = [c1h["open_date"] - (1*_1h), c1h["close_date"] - (1*_1h)]
        c1d = last(pair,"1d")
        t1d = [c1d["open_date"] - (1*_1d), c1d["close_date"] - (1*_1d)]

        for n in range(1,4):
            dfsig = dfsig.append([
                sigstr(pair, "5m", str(n*60)+"m", t5m[0]-(n*60*_1m), end=t5m[1]),
                sigstr(pair, "1h", str(n*24)+"h", t1h[0]-(n*24*_1h), end=t1h[1]),
                sigstr(pair, "1d", str(n*7)+"d",  t1d[0]-(n*7*_1d), end=t1d[1])
            ])

    # Sum signals in last column for each multi-index (Pair,Freq,Period,Prop)
    dfsig = dfsig.sort_index()
    sumlist = [ [n[0:-1], dfsig.ix[n[0:-1]]["Signal"].sum()] for n in dfsig.index.values]
    sumdict = {}
    for n in sumlist:
        sumdict[n[0]] = n[1]

    dfsum = pd.DataFrame(
        list(sumdict.values()),
        index=pd.MultiIndex.from_tuples(list(sumdict.keys())),
        columns=["Signal"]
    ).sort_index()

    if dbstore:
        store(dfsum, dfsig)

    return {"signalsum":dfsum, "signals":dfsig}

    """df = df.sort_values([('5m','60m')], ascending=False)
    max5m = df["5m"]["60m"].idxmax()
    df5m = pd.DataFrame([df.ix[max5m].values], index=[max5m], columns=df.columns)
    df = df.sort_values([('1h','24h')], ascending=False)
    max1h = df["1h"]["24h"].idxmax()
    df1h = pd.DataFrame([df.ix[max1h].values], index=[max1h], columns=df.columns)
    #, "max5m":df5m, "max1h":df1h}
    """

#-----------------------------------------------------------------------------
def store(dfsum, dfsig):
    db = get_db()

    ops=[]
    # Save signal sum data
    for key in list(dfsum.to_dict()["Signal"].keys()):
        sigval = float(dfsum.ix[key]["Signal"])
        since = now() if sigval > 0 else 0
        ops.append(UpdateOne(
            {"pair":key[0], "freq":key[1], "period":key[2]},
            {
              "$set": {"pair":key[0], "freq":key[1], "period":key[2], "signal":sigval},
              "$min": {"since":since},
            },
            upsert=True))
    r = db.signal_sum.bulk_write(ops)
    log.debug(r.bulk_api_result)

    ops=[]
    # Save signal data
    for key in dfsig.index.values:
        k = key[0:-1]
        ops.append(ReplaceOne(
            {"pair":k[0], "freq":k[1], "period":k[2]},
            {"pair":k[0], "freq":k[1], "period":k[2], "dataframe":dfsig.ix[k].to_dict()},
            upsert=True))
    r = db.signals.bulk_write(ops)
    log.debug(r.bulk_api_result)

#-----------------------------------------------------------------------------
def sigstr(pair, freq, label, start, end):
    """Compare candle fields to historical averages. Measure magnitude in
    number of standard deviations from the mean.
    """
    dfc = db_get(pair, freq, None)
    dfc = dfc.to_dict('record')[0]
    dfh = db_get(pair, freq, start, end=end)
    havg = dfh.describe()[1::]
    data = []

    # Price diff in past hour
    close_1h = dfc["close"] - dfh["close"].ix[-1]

    # Price vs hist. mean/std
    c_c = dfc["close"]
    h_c = havg["close"]
    cd = c_c - h_c["mean"]
    cs = cd / h_c["std"]
    data.append([c_c, h_c["mean"], cd, h_c["std"], cs])

    # Volume vs hist. mean/std
    c_v = dfc["volume"]
    h_v = havg["volume"]
    vd = c_v - h_v["mean"]
    vs = vd / h_v["std"]
    data.append([c_v, h_v["mean"], vd, h_v["std"], vs])

    # Buy volume vs hist. mean/std
    c_bv = dfc["buy_vol"]
    h_bv = havg["buy_vol"]
    bvd = c_bv - h_bv["mean"]
    bvs = bvd / h_bv["std"]
    data.append([c_bv, h_bv["mean"], bvd, h_bv["std"], bvs])

    # Buy/sell volume ratio vs hist. mean/std
    c_br = dfc["buy_ratio"]
    h_br = havg["buy_ratio"]
    brd = c_br - h_br["mean"]
    brs = brd / h_br["std"]
    data.append([c_br, h_br["mean"], brd, h_br["std"], brs])

    # Number trades vs hist. mean/std
    c_t = dfc["trades"]
    h_t = havg["trades"]
    td = c_t - h_t["mean"]
    ts = td / h_t["std"]
    data.append([c_t, h_t["mean"], td, h_t["std"], ts])

    score = cs + vs + bvs + brs + ts
    score = round(float(score), 2)

    fields = ["Close", "Volume", "BuyVol", "BuyRatio", "Trades"]
    cols = ["Candle", "HistMean", "Diff", "HistStd", "Signal"]

    # 4-level multi-index [5 x 5] dataframe
    # i.e. BTCUSDT->1H->24H->Volume
    df = pd.DataFrame(
        data,
        index=pd.MultiIndex.from_product([ [pair], [freq], [label], fields ]),
        columns=cols
    ).astype(float).round(7)
    return df

#-----------------------------------------------------------------------------
def _print(sigresult):
    from app.utils import to_local
    cndl = sigresult["candle"]
    op = to_local(cndl["open_date"])
    cl = to_local(cndl["close_date"])
    rng = to_local(sigresult["hist_rng"])

    print("Pair: %s" % cndl["pair"])
    print("Freq: %s" % cndl["freq"])
    print("Candle: %s-%s %s" % (op.time().strftime("%H:%M:%S"),
        cl.time().strftime("%H:%M:%S"), cl.strftime("%Z")))
    print("Timespan: %s" %(rng[1]) - rng[0])
    print("\n%s" % sigresult["df"])
    print("\nSignal: %s" % sigresult["score"])
    print("%s" % ("-"*75))
