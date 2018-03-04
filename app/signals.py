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
from pymongo import ReplaceOne
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
def gsigstr(mute=False, dbstore=False):
    """Get signal dataframe for all combined pairs.
    """
    _1m = timedelta(minutes=1)
    _1h = timedelta(hours=1)
    _1d = timedelta(hours=24)
    if mute:
        sys.stdout=None
    sigs=[]
    df3=pd.DataFrame()
    # For each pair, for each freq, get set of signals for varying timeframes.
    for pair in BINANCE["CANDLES"]:
        try:
            res = {"pair":pair, "5m":[], "1h":[], "1d":[]}
            c5m = last(pair,"5m")
            t5m = [c5m["open_date"] - (5*_1m), c5m["close_date"] - (5*_1m)]
            c1h = last(pair,"1h")
            t1h = [c1h["open_date"] - (1*_1h), c1h["close_date"] - (1*_1h)]
            c1d = last(pair,"1d")
            t1d = [c1d["open_date"] - (1*_1d), c1d["close_date"] - (1*_1d)]
            for n in range(1,4):
                res["5m"] += [sigstr(pair, "5m", str(n*60)+"m", t5m[0] - (n*60*_1m), end=t5m[1])]
                res["1h"] += [sigstr(pair, "1h", str(n*24)+"h",  t1h[0] - (n*24*_1h), end=t1h[1])]
                res["1d"] += [sigstr(pair, "1d", str(n*7)+"d",  t1d[0] - (n*7*_1d), end=t1d[1])]

            df3 = df3.append(res["5m"][0]["df"]).append(res["5m"][1]["df"]
            ).append(res["5m"][2]["df"]).append(res["1h"][0]["df"]
            ).append(res["1h"][1]["df"]).append(res["1h"][2]["df"]
            ).append(res["1d"][0]["df"]).append(res["1d"][1]["df"]
            ).append(res["1d"][2]["df"])
        except Exception as e:
            log.exception(str(e))
            continue
        sigs += [res]

    df3.index.names = ["Pair","Freq","Range",""]

    if mute:
        sys.stdout = sys.__stdout__

    if dbstore:
        store(sigs)

    # Combine all pair signals into dataframe, find strongest signals
    df = pd.DataFrame(
        index=BINANCE["CANDLES"],
        columns=[
            ["5m","5m","5m","1h","1h","1h","1d","1d","1d"],
            ["60m","120m","180m","24h","48h","72h","7d","14d","21d"]
        ]
    ).astype(float).round(2)
    for psigs in sigs:
        df.loc[psigs["pair"]] = [n["score"] for n in psigs["5m"]] +\
            [n["score"] for n in psigs["1h"]] +\
            [n["score"] for n in psigs["1d"]]

    update_sigtracker(df)

    df = df.sort_values([('5m','60m')], ascending=False)
    max5m = df["5m"]["60m"].idxmax()
    df5m = pd.DataFrame([df.ix[max5m].values], index=[max5m], columns=df.columns)

    df = df.sort_values([('1h','24h')], ascending=False)
    max1h = df["1h"]["24h"].idxmax()
    df1h = pd.DataFrame([df.ix[max1h].values], index=[max1h], columns=df.columns)

    return {"signalsum":df, "signals":df3, "max5m":df5m, "max1h":df1h}

#-----------------------------------------------------------------------------
def store(gsiglist):
    """Batch store list of sigstr results to mongodb.
    :gsiglist: list of signal dicts in format: {
        "pair":pair,
        "5m":[sigstr_results],
        "1h":[sigstr_results]
    }
    """
    t1 = Timer()
    ops=[]
    for pairsigs in gsiglist:
        for sig in pairsigs["5m"]:
            sig["df"] = sig["df"].to_dict()
        for sig in pairsigs["1h"]:
            sig["df"] = sig["df"].to_dict()
        for sig in pairsigs["1d"]:
            sig["df"] = sig["df"].to_dict()
        ops.append(ReplaceOne({"pair":pairsigs["pair"]}, pairsigs,
            upsert=True))
    try:
        result = get_db().signals.bulk_write(ops)
    except Exception as e:
        return log.exception("gsigstr bulk_write() error")

    log.debug("pair signals saved to db in %sms", t1)
    log.debug(result.bulk_api_result)

#-----------------------------------------------------------------------------
def update_sigtracker(df):
    """Update summary signal document w/ running timer that each
    pair-frequency-score is > 0 (longer time, higher confidence in signal).
    """
    from app.utils import utc_datetime
    pairs = df.index.tolist()
    records = df.to_dict('record')
    db = get_db()
    ops = []
    docs = []

    for n in range(0,len(pairs)):
        pair = pairs[n]
        r = records[n]
        doc = {"pair":pair}
        _saved = db.sig_tracker.find_one({"pair":pair})

        # k is multi-index tuple ('5m', '60m')
        for k in list(r.keys()):
            key = "_".join(k)
            score = r[k]

            if score > 0:
                if _saved and _saved.get(key) and _saved[key][0] > 0:
                    # Keep existing +score datetime, update score
                    doc[key] = [score, _saved[key][1]]
                else:
                    # Add new +score datetime
                    doc[key] = [score, utc_datetime()]
            else:
                doc[key] = [score]

        docs.append(doc)
        ops.append(ReplaceOne({"pair":pair}, doc, upsert=True))

    result = db.sig_tracker.bulk_write(ops)
    return docs

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

    df = pd.DataFrame(data,
        index=["Close", "Volume", "BuyVol", "BuyRatio", "Trades"],
        columns=["Candle", "HistMean", "Diff", "HistStd", "Score"]
    ).astype(float).round(7)

    df = pd.concat([df], keys=[(pair,freq.upper(),label.upper())])

    return {
        "hist_rng":[dfh["close_date"].ix[0], dfh["close_date"].ix[-1]],
        "df":df,
        "score":score,
        "candle":dfc
    }

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

#------------------------------------------------------------------------------
def get_sig(pair):
    """https://pandas.pydata.org/pandas-docs/stable/advanced.html
    """
    sig = get_db().signals.find_one({"pair":pair})
    return sig

    """
    for pairsigs in sigs:
        _5m = pairsigs["5m"]
        _1h = pairsigs["1h"]
        pair = pairsigs["pair"]#.lower()

        _df1 = pd.concat([
                _5m[0]["df"],
                _5m[1]["df"],
                _5m[2]["df"],
                _1h[0]["df"],
                _1h[1]["df"],
                _1h[2]["df"]
            ],
            keys={
                "%s_%s"%(pair,"A_60m"): _5m[0]["df"],
                "%s_%s"%(pair,"B_120m"):_5m[1]["df"],
                "%s_%s"%(pair,"C_180m"):_5m[2]["df"],
                "%s_%s"%(pair,"D_24h"):_1h[0]["df"],
                "%s_%s"%(pair,"E_48h"):_1h[1]["df"],
                "%s_%s"%(pair,"F_72h"):_1h[2]["df"]
            }
        )#.sort_index()
        print("")
        print(_df1)

        #_df2 = pd.concat(_1h, keys={"24H":_1h[0], "48H":_1h[1], "72H":_1h[2]})
        #_bigdf = pd.concat([_df1,_df2], keys={pairsigs["pair"]+"_5m":_df1, pairsigs["pair"]+"_1h":_df2})
        #print(_bigdf)
    """
