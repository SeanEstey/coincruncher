# app.signals
import logging
import sys
from config import BINANCE_PAIRS
from pymongo import ReplaceOne
from datetime import timedelta
from pprint import pprint
import pandas as pd
import numpy as np
from app import get_db
from app.candles import db_get, last
from app.timer import Timer
from app.utils import utc_datetime as now
log = logging.getLogger('signals')

#------------------------------------------------------------------------------
def gsigstr(mute=False, store=False):
    _1m = timedelta(minutes=1)
    _1h = timedelta(hours=1)

    if mute:
        sys.stdout=None

    df = pd.DataFrame(
        index=BINANCE_PAIRS,
        columns=[
            "5m_vs_60m", " 5m_vs_120m", " 5m_vs_180m",
            "1h_vs_24h", " 1h_vs_48h", "1h_vs_72h",
        ]
    ).astype(float).round(2)

    sigs=[]

    # For each pair, for each freq, get set of signals for varying timeframes.
    for pair in BINANCE_PAIRS:
        try:
            res = {"pair":pair, "5m":[], "1h":[]}

            c5m = last(pair,"5m")
            time5m = [c5m["open_date"] - (5*_1m), c5m["close_date"] - (5*_1m)]
            c1h = last(pair,"1h")
            time1h = [c1h["open_date"] - (1*_1h), c1h["close_date"] - (1*_1h)]

            for n in range(1,4):
                res["5m"] += [sigstr(pair, "5m", time5m[0] - (n * 60 *_1m), end=time5m[1])]
                res["1h"] += [sigstr(pair, "1h", time1h[0] - (n * 24 *_1h), end=time1h[1])]
        except Exception as e:
            log.exception(str(e))
            continue
        sigs += [res]

    if mute:
        sys.stdout = sys.__stdout__

    t1 = Timer()

    # Save to db (optional)
    if store:
        ops=[]
        for pairsigs in sigs:
            for sig in pairsigs["5m"]:
                sig["df"] = sig["df"].to_dict()
            for sig in pairsigs["1h"]:
                sig["df"] = sig["df"].to_dict()

            ops.append(
                ReplaceOne({"pair":pairsigs["pair"]}, pairsigs, upsert=True)
            )
        try:
            db_res = get_db().signals.bulk_write(ops)
        except Exception as e:
            return log.exception("gsigstr bulk_write() error")

        log.debug("pair signals saved to db in %sms", t1)
        log.debug(db_res.bulk_api_result)

    # Find strongest signals
    for psigs in sigs:
        df.loc[psigs["pair"]] = [n["score"] for n in psigs["5m"]] + [n["score"] for n in psigs["1h"]]
    df = df[df > 0]
    print("SIGMAX")

    _5m_pair = df["5m_vs_60m"].idxmax()
    _5m_max = df["5m_vs_60m"].max()
    _5m_sigstr = df.ix[_5m_pair].tolist()[0:3]
    _5m_sigresult = [ n["5m"] for n in sigs if n["pair"] == _5m_pair][0]
    print("Freq: 5m, Pair: %s, Sigstr: %s" %(_5m_pair, _5m_sigstr))

    _1h_pair = df["1h_vs_24h"].idxmax()
    _1h_max = df["1h_vs_24h"].max()
    _1h_sigstr = df.ix[_1h_pair].tolist()[3:6]
    _1h_sigresult = [ n["1h"] for n in sigs if n["pair"] == _1h_pair][0]
    print("Freq: 1h, Pair: %s, Sigstr: %s" %(_1h_pair, _1h_sigstr))

    df = df.replace(np.NaN,"-")

    return {
        "df":df,
        "5m_max": "MAX_5m: %s" % _5m_pair,
        "1h_max": "MAX_1h: %s" % _1h_pair,
        "1h_sigresult":_1h_sigresult,
        "5m_sigresult":_5m_sigresult
    }

#-----------------------------------------------------------------------------
def _print(sigresult):
    from app.utils import to_local
    pair = sigresult["candle"]["pair"]
    freq = sigresult["candle"]["freq"]

    print("Pair: %s" % pair)
    print("Freq: %s" % freq)
    print("Hist: %s - %s %s" %(
        to_local(sigresult["hist_rng"][0]).strftime("%m-%dT%H:%M"),
        to_local(sigresult["hist_rng"][1]).strftime("%m-%dT%H:%M"),
        to_local(sigresult["hist_rng"][1]).strftime("%Z"))
    )
    print("Candle: %s-%s %s" % (
        to_local(sigresult["candle"]["open_date"]).time().strftime("%H:%M:%S"),
        to_local(sigresult["candle"]["close_date"]).time().strftime("%H:%M:%S"),
        to_local(sigresult["candle"]["close_date"]).strftime("%Z"))
    )
    print("\n%s" % sigresult["df"])
    print("\nSignal: %s" % sigresult["score"])
    print("%s" % ("-"*75))

#-----------------------------------------------------------------------------
def sigstr(pair, freq, start, end):
    """Compare candle fields to historical averages. Measure magnitude in
    number of standard deviations from the mean.

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
    confidence in the signal.

    Example:
        1) Volume traded in the last hour was >= 3 STD over mean; AND
        2) Change in price over the last hour is >=2 STD over mean; AND
        3) Volume ratio of offer/bid is >= 2 STD over mean

    The model will take in factors over multiple time periods (i.e. minute,
    hourly, daily, weekly) and will also compare against multiple
    historical time periods (i.e. volume over last 24 hours, 48 hours, 72 hours)

    Furthermore CoinFi stores these factors across a selected universe of coins
    and the model is continuously run, outputting alerts when a factor or a
    series of factors has a positive signal.
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
    #cs = max(0, cd / h_c["std"])
    # ALLOW NEGATIVE SCORES
    cs = cd / h_c["std"]
    data.append([c_c, h_c["mean"], cd, h_c["std"], cs])

    # Volume vs hist. mean/std
    c_v = dfc["volume"]
    h_v = havg["volume"]
    vd = c_v - h_v["mean"]
    #vs = max(0, vd / h_v["std"])
    # ALLOW NEGATIVE SCORES
    vs = vd / h_v["std"]
    data.append([c_v, h_v["mean"], vd, h_v["std"], vs])

    # Buy volume vs hist. mean/std
    c_bv = dfc["buy_vol"]
    h_bv = havg["buy_vol"]
    bvd = c_bv - h_bv["mean"]
    #bvs = max(0, bvd / h_bv["std"])
    # ALLOW NEGATIVE SCORES
    bvs = bvd / h_bv["std"]
    data.append([c_bv, h_bv["mean"], bvd, h_bv["std"], bvs])

    # Buy/sell volume ratio vs hist. mean/std
    c_br = dfc["buy_ratio"]
    h_br = havg["buy_ratio"]
    brd = c_br - h_br["mean"]
    #brs = max(0, brd / h_br["std"])
    # ALLOW NEGATIVE SCORES
    brs = brd / h_br["std"]
    data.append([c_br, h_br["mean"], brd, h_br["std"], brs])

    # Number trades vs hist. mean/std
    c_t = dfc["trades"]
    h_t = havg["trades"]
    td = c_t - h_t["mean"]
    #ts = max(0, td / h_t["std"])
    # ALLOW NEGATIVE SCORES
    ts = td / h_t["std"]
    data.append([c_t, h_t["mean"], td, h_t["std"], ts])

    score = cs + vs + bvs + brs + ts
    score = round(float(score), 2)

    df = pd.DataFrame(data,
        index=["Close", "Volume", "Buy Vol", "Buy Ratio", "Trades"],
        columns=["Candle", "Hist. Mean", "Diff", "Hist. Std", "Score"]
    ).astype(float).round(7)

    print("Pair: %s" % pair)
    print("Freq: %s" % freq)
    print("Hist: %s to %s" %(
        dfh["close_date"].ix[0].strftime("%b-%d %H:%M"),
        dfh["close_date"].ix[-1].strftime("%b-%d %H:%M")))
    print("Candle: %s to %s UTC" % (
        dfc["open_date"].time().strftime("%H:%M:%S"),
        dfc["close_date"].time().strftime("%H:%M:%S")))
    print("\n%s" % df)
    print("\nSignal: %s" % score)
    print("%s" % ("-"*75))

    return {
        "hist_rng":[dfh["close_date"].ix[0], dfh["close_date"].ix[-1]],
        "df":df,
        "score":score,
        "candle":dfc
    }
