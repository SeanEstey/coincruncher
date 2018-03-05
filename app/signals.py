# app.signals
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
def calc_aggr(to_db=False):
    """Aggregate signal scores for all Binance pair candles.
    """
    _1m = timedelta(minutes=1)
    _1h = timedelta(hours=1)
    _1d = timedelta(hours=24)
    df_pairs = pd.DataFrame()

    # Generate signal scores for each (Pair,Freq,Period) tuple.
    for pair in BINANCE["CANDLES"]:
        c5m = last(pair,"5m")
        t5m = [c5m["open_date"] - (5*_1m), c5m["close_date"] - (5*_1m)]
        c1h = last(pair,"1h")
        t1h = [c1h["open_date"] - (1*_1h), c1h["close_date"] - (1*_1h)]
        c1d = last(pair,"1d")
        t1d = [c1d["open_date"] - (1*_1d), c1d["close_date"] - (1*_1d)]

        for n in range(1,4):
            df_pairs = df_pairs.append([
                calc_pair(pair, "5m", str(n*60)+"m", t5m[0]-(n*60*_1m), end=t5m[1]),
                calc_pair(pair, "1h", str(n*24)+"h", t1h[0]-(n*24*_1h), end=t1h[1]),
                calc_pair(pair, "1d", str(n*7)+"d",  t1d[0]-(n*7*_1d), end=t1d[1])
            ])

    # Sum signals in last column for each multi-index (Pair,Freq,Period,Prop)
    df_pairs = df_pairs.sort_index()
    aggr_list = [ [n[0:-1], df_pairs.ix[n[0:-1]]["Signal"].sum()] for n in df_pairs.index.values]
    aggr_dict = {}
    for n in aggr_list:
        aggr_dict[n[0]] = n[1]

    df_aggr = pd.DataFrame(
        list(aggr_dict.values()),
        index=pd.MultiIndex.from_tuples(list(aggr_dict.keys())),
        columns=["Signal"]
    ).sort_index()

    if to_db:
        save_db(df_aggr=df_aggr, df_pairs=df_pairs)

    return {"aggregate":df_aggr, "pairs":df_pairs}

#-----------------------------------------------------------------------------
def calc_pair(pair, freq, label, start, end):
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
    return pd.DataFrame(
        data,
        index=pd.MultiIndex.from_product([ [pair], [freq], [label], fields ]),
        columns=cols
    ).astype(float).round(7)

#-----------------------------------------------------------------------------
def load_db(aggr=True, pairs=False):
    """Return aggregate signals from db as multi-index dataframe.
    """
    if pairs == True:
        # TODO
        pass

    if aggr == True:
        df = pd.DataFrame(list(get_db().signal_sum.find()))
        df.index = list(zip(df.pair, df.freq, df.period))
        df.since = df.since.replace(0,np.nan)
        _df = pd.DataFrame(
            df[["signal","since"]],
            index=pd.MultiIndex.from_tuples(df.index)
        ) #.sort_index()
        _df = _df.sort_values(by=['signal'], ascending=False)
        return _df

#-----------------------------------------------------------------------------
def save_db(df_aggr=None, df_pairs=None):
    db = get_db()

    if df_aggr is not None:
        ops=[]
        # Save signal sum data
        for key in list(df_aggr.to_dict()["Signal"].keys()):
            sigval = float(df_aggr.ix[key]["Signal"])
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

    if df_pairs is not None:
        ops=[]
        # Save signal data
        for key in df_pairs.index.values:
            k = key[0:-1]
            ops.append(ReplaceOne(
                {"pair":k[0], "freq":k[1], "period":k[2]},
                {"pair":k[0], "freq":k[1], "period":k[2], "dataframe":df_pairs.ix[k].to_dict()},
                upsert=True))
        r = db.signals.bulk_write(ops)
        log.debug(r.bulk_api_result)

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
