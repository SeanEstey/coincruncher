# app.signals
import logging
import sys
from config import BINANCE_PAIRS
from datetime import timedelta
from pprint import pprint
import pandas as pd
import numpy as np
from app import get_db
from app.candles import db_get
from app.utils import utc_datetime as now
log = logging.getLogger('signals')

#------------------------------------------------------------------------------
def gsigstr(mute=False):
    if mute:
        sys.stdout=None

    """columns=[
        " 5m.vs.60m", " 5m.vs.120m", " 5m.vs.180m",
        " 1h.vs.24hr", " 1h.vs.48hr", " 1h.vs.72hr",
    ]
    """
    df = pd.DataFrame(
        index=BINANCE_PAIRS,
        columns=[
            "5m.t-1", "5m.t-2", "5m.t-3",
            "1h.t-1", "1h.t-2", "1h.t-3"
        ]
    ).astype(float).round(2)

    for pair in BINANCE_PAIRS:
        try:
            s5m = multisigstr(pair, "5m")
            s1h = multisigstr(pair, "1h")
            df.loc[pair] = [ s5m[0], s5m[1], s5m[2], s1h[0], s1h[1], s1h[2] ]
        except Exception as e:
            log.exception(str(e))
            continue

    #print(df)

    if mute:
        sys.stdout = sys.__stdout__

    df = df[df > 0]

    print("SIGMAX")
    _5m_pair = df["5m.t-1"].idxmax()
    _5m_max = df["5m.t-1"].max()
    _5m_sigstr = df.ix[_5m_pair].tolist()[0:3]
    print("Freq: 5m, Pair: %s, Sigstr: %s" %(_5m_pair, _5m_sigstr))
    _1h_pair = df["1h.t-1"].idxmax()
    _1h_max = df["1h.t-1"].max()
    _1h_sigstr = df.ix[_1h_pair].tolist()[3:6]
    print("Freq: 1h, Pair: %s, Sigstr: %s" %(_1h_pair, _1h_sigstr))

    df = df.replace(np.NaN,"-")
    return df

#------------------------------------------------------------------------------
def multisigstr(pair, freq):
    """Average signal strength from 3 different historical average
    time spans.
    """
    freqlen=None
    periodlen=None
    microsec = timedelta(microseconds=1)

    if freq == "1h":
        periodlen = timedelta(hours=24)
        freqlen = timedelta(hours=1)
    elif freq == "5m":
        periodlen = timedelta(hours=1)
        freqlen = timedelta(minutes=5)

    scores = [
        sigstr(pair, freq, now()-periodlen, end=now()-microsec),
        sigstr(pair, freq, now()-(2*periodlen), end=now()-periodlen-microsec),
        sigstr(pair, freq, now()-(3*periodlen), end=now()-(2*periodlen)-microsec)
    ]

    # sigstr(pair, freq, now() - tspan*2 - periodlen, end=now()),
    # sigstr(pair, freq, now() - tspan*1 - periodlen, end=now())

    return scores

    #avg_score = round(sum(scores) / len(scores), 2)
    #print("\n%s %s Avg Signal Score: %s" %(pair, freq, avg_score))
    #return avg_score

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

    Example: Volume traded in the last hour was 3 standard deviations above the mean; AND
    Change in price over the last hour is 2 standard deviations above the mean; AND
    Volume ratio of offer/bid is 2 standard deviations above the mean.
    we will have greater confidence in the signal.

    The model will take in factors over multiple time periods (i.e. minute,
    hourly, daily, weekly) and will also compare against multiple historical
    time periods (i.e. volume over last 24 hours, 48 hours, 72 hours)

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

    return score
    """
    return {"havg":havg,
            "df":df,
            "score":score}
    """
