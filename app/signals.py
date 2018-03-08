# app.signals
import logging
from pymongo import ReplaceOne, UpdateOne
from datetime import timedelta
import pandas as pd
import numpy as np
from app import get_db
from app.candles import db_get, last
from app.timer import Timer
from app.utils import utc_datetime as now, to_float, parse_period as per_to_sec
from docs.data import BINANCE
log = logging.getLogger('signals')

#------------------------------------------------------------------------------
def calculate_all():
    """Compute pair and aggregate signal data for Binance candles.
    """
    timer = Timer()
    _1m = timedelta(minutes=1)
    _1h = timedelta(hours=1)
    _1d = timedelta(hours=24)
    dfp = pd.DataFrame()

    # Generate signal scores for each (Pair,Freq,Period) tuple.
    for pair in BINANCE["CANDLES"]:
        c5m = last(pair,"5m")
        t5m = [c5m["open_date"] - (5*_1m), c5m["close_date"] - (5*_1m)]
        c1h = last(pair,"1h")
        t1h = [c1h["open_date"] - (1*_1h), c1h["close_date"] - (1*_1h)]
        c1d = last(pair,"1d")
        t1d = [c1d["open_date"] - (1*_1d), c1d["close_date"] - (1*_1d)]

        for n in range(1,4):
            dfp = dfp.append([
                calculate(pair, "5m", str(n*60)+"m", t5m[0]-(n*60*_1m), end=t5m[1]),
                calculate(pair, "1h", str(n*24)+"h", t1h[0]-(n*24*_1h), end=t1h[1]),
                calculate(pair, "1d", str(n*7)+"d",  t1d[0]-(n*7*_1d), end=t1d[1])
            ])

    dfa = pd.DataFrame(dfp.groupby(level=[0,1,2]).sum()["Signal"].round(2))

    save_db_aggregate(dfa)
    save_db_pairs(dfp)

    #baller = pd.DataFrame(dfa[dfa.signal >= 2][dfa.flip_date != False].max())
    #log.log(100, "%s is a ballin' with %s signal on %s/%s",
    #baller.index.values[0], baller.signal, baller.index.values[1],
    #baller.index.values[2])

    log.debug("calculate_all completed in %sms", timer)
    return dfa

#-----------------------------------------------------------------------------
def calculate(pair, freq, period, start, end):
    """Compare candle fields to historical averages. Measure magnitude in
    number of standard deviations from the mean.
    """
    dfc = db_get(pair, freq, None)
    dfc = dfc.to_dict('record')[0]
    dfh = db_get(pair, freq, start, end=end)
    havg = dfh.describe()[1::]
    data = []

    # Price diff in past hour
    #close_1h = dfc["close"] - dfh["close"].loc[-1]

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
    _freq = int(per_to_sec(freq)[2].total_seconds())
    _period = int(per_to_sec(period)[2].total_seconds())

    dfp = pd.DataFrame(
        data,
        index=pd.MultiIndex.from_product([ [pair], [_freq], [_period], fields ]),
        columns=cols
    ).astype(float).round(7)

    dfp.index.names=["pair","freq","period","prop"]
    return dfp

#-----------------------------------------------------------------------------
def save_db_pairs(dfp):
    timer = Timer()
    db = get_db()
    ops=[]

    # Save signal data
    for key in dfp.index.values:
        k = key[0:-1]
        values = dfp.loc[k].values.tolist()
        ops.append(ReplaceOne(
            {"pair":k[0], "freq":k[1], "period":k[2]},
            {"pair":k[0], "freq":k[1], "period":k[2], "data":values},
            upsert=True))

    res = db.pair_signals.bulk_write(ops)
    log.debug("%s pair signals saved to db in %sms", res.modified_count, timer)

#-----------------------------------------------------------------------------
def load_db_pairs():
    """Load pair signal data from DB as multi-index dataframe.
    Returns:
        aggregate signals dataframe
            multi-index levels:
                0:"Pair"
                1:"Freq"
                2:"Period"
                3:"Prop"   # Candle property
            columns:
                ["Candle", "HistMean", "Diff", "HistStd", "Signal"]
    """
    # Fill w/ index values, use to build multi-index df
    idx_values=[]
    # Candle properties for "Prop" index
    cndl_prop=["close", "volume", "buy_vol", "buy_ratio", "trades"]
    # Fill each sublist w/ column data
    data=[[],[],[],[],[]]

    for item in get_db().pair_signals.find():
        for i in range(0,5):
            idx_values.append(
                (item["pair"], item["freq"], item["period"], cndl_prop[i])
            )
            for j in range(0,5):
                data[i].append(item["data"][j][i])

    col_names=["candle", "hist_mean", "diff", "hist_std", "signal"]

    dfp = pd.DataFrame(
        data = { col_names[n]:data[n] for n in range(0,5) },
        index = pd.MultiIndex.from_tuples(idx_values),
        columns = col_names
    ).sort_index()
    dfp.index.names = ["pair","freq","period","prop"]
    dfp.signal = dfp.signal.round(2)

    log.debug("loaded df with %s indices from db.pair_signals", len(dfp))
    return dfp

#-----------------------------------------------------------------------------
def save_db_aggregate(dfa):
    t = Timer()
    sign = lambda x: -1 if x<0 else 1 if x>0 else 0
    dfa.index.names = [x.lower() for x in dfa.index.names]
    dfa.columns = [x.lower() for x in dfa.columns]
    last_dfa = load_db_aggregate()

    if last_dfa is None:
        dfa["last_signal"] = False
        dfa["flip_date"] = False
        return get_db().aggr_signals.bulk_write([
            UpdateOne(
                dict(zip(["pair", "freq", "period"], idx)),
                {"$set":dfa.loc[idx].to_dict()},
                upsert=True) for idx in dfa.index.values
        ])
    else:
        dfa["last_signal"] = last_dfa["signal"]
        dfa["flip_date"] = last_dfa["flip_date"]
        dfa["flip_date"] = dfa["flip_date"].replace(np.nan,False)
        dfa["is_flip"] = dfa["signal"].apply(sign) != dfa["last_signal"].apply(sign)
        dfa.loc[dfa[dfa.is_flip==True].index, 'flip_date'] = now()
        del dfa["is_flip"]

        try:
            res = get_db().aggr_signals.bulk_write([
                UpdateOne(
                    dict(zip(["pair", "freq", "period"], idx)),
                    {"$set":dfa.loc[idx].to_dict()},
                    upsert=True) for idx in dfa.index.values
            ])
        except Exception as e:
            log.exception("bulk_write: %s", str(e))
        else:
            log.debug("%s aggregate signals saved to DB [%sms]", res.modified_count, t)
            return dfa

#-----------------------------------------------------------------------------
def load_db_aggregate(title=None):
    """Load aggregate signal data from DB as multi-index dataframe.
    Returns:
        pair signals dataframe
            multi-index levels:
                0:"pair",
                1:"freq",
                2:"period",
            columns:
                ["signal", "flip_date"]
    """
    df = pd.DataFrame(
        list(get_db().aggr_signals.find()))

    if len(df) == 0:
        return None

    df.index = pd.MultiIndex.from_tuples(
        list(zip(df.pair, df.freq, df.period)))

    dfa = df[["flip_date", "signal"]]
    dfa.index.names = ["pair","freq","period"]

    log.debug("loaded df with %s indices from db.aggr_signals", len(dfa))
    return dfa
