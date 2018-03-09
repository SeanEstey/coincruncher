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

#baller = pd.DataFrame(dfa[dfa.signal >= 2][dfa.flip_date != False].max())
#log.log(100, "%s is a ballin' with %s signal on %s/%s",
#baller.index.values[0], baller.signal, baller.index.values[1],
#baller.index.values[2])

#------------------------------------------------------------------------------
def siglog(dfa, dfp):
    for freq in list(dfa.index.levels[1]):
        aggr_df = dfa.xs(freq, level=1).sort_values('signal', ascending=False)
        aggr_sigmax = aggr_df.signal.max()

        # (pair, period)
        pair = aggr_df.iloc[0].name[0]
        period = aggr_df.iloc[0].name[1]
        flip_date =  aggr_df.iloc[0].flip_date
        flip_msg = ""
        if flip_date:
            diff = (now() - flip_date).total_seconds()
            flip_msg = ", above zero for %sh" % str(round(diff/3600,1))

        dfp_max = dfp.loc[(pair, freq, period)]
        pair_prop = dfp_max.signal.idxmax()
        pair_prop_val = dfp_max.loc[pair_prop]['signal']


        log.log(100,
            "%s %s is STRONG at %sx mean%s. SIG=+%s",
            pair,
            pair_prop.title().replace('_',''),
            round(pair_prop_val,1),
            flip_msg,
            aggr_sigmax)

#------------------------------------------------------------------------------
def calculate_all():
    """Compute pair/aggregate signal data. Binance candle historical averages..
    """
    t = Timer()
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

    dfa = aggregate(dfp.copy())
    save_db(aggr=dfa, pairs=dfp)

    log.info("Binance signals updated. n=%s, t=%sms", len(dfp)+len(dfa), t)
    return [dfa, dfp]

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

    fields = ["close", "volume", "buy_vol", "buy_ratio", "trades"]
    cols = ["candle", "hist_mean", "diff", "hist_std", "signal"]
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
def aggregate(dfp):
    t = Timer()
    db = get_db()
    sign = lambda x: -1 if x<0 else 1 if x>0 else 0

    dfa = pd.DataFrame(dfp.groupby(level=[0,1,2]).sum()["signal"].round(2))
    dfa_prv = load_db_aggr()

    if dfa_prv is not None:
        dfa["last_signal"] = dfa_prv["signal"]
        dfa["flip_date"] = dfa_prv["flip_date"].replace(np.nan, False)
        dfa["is_flip"] = dfa["signal"].apply(sign) != dfa["last_signal"].apply(sign)
        dfa.loc[dfa[dfa.is_flip==True].index, 'flip_date'] = now()
        del dfa["is_flip"]
    else:
        dfa["last_signal"] = False
        dfa["flip_date"] = False

    return dfa

#-----------------------------------------------------------------------------
def save_db(aggr=None, pairs=None):
    """Given pair signal dataframe, Generate aggregate (sum) signals on each
    index (pair, freq, period, prop), along with time since last sign change,
    t(signal > 0) and t(signal < 0).
    #dfa.index.names = [x.lower() for x in dfa.index.names]
    #dfa.columns = [x.lower() for x in dfa.columns]
    """
    t = Timer()
    db = get_db()

    if aggr is not None:
        dfa = aggr
        bulk=[]
        for idx in dfa.index.values:
            bulk.append(UpdateOne(
                dict(zip(["pair", "freq", "period"], idx)),
                {"$set":dfa.loc[idx].to_dict()},
                upsert=True))
        try:
            res = db.aggr_signals.bulk_write(bulk)
        except Exception as e:
            return log.exception(str(e))

        log.debug("%s aggr_sigs stored [%sms]", res.modified_count, t)

    if pairs is not None:
        dfp = pairs
        bulk=[]
        for key in dfp.index.values:
            k = key[0:-1]
            values = dfp.loc[k].values.tolist()

            bulk.append(ReplaceOne(
                {"pair":k[0], "freq":k[1], "period":k[2]},
                {"pair":k[0], "freq":k[1], "period":k[2], "data":values},
                upsert=True))
        try:
            res = db.pair_signals.bulk_write(bulk)
        except Exception as e:
            return log.exception(str(e))

        return log.debug("%s pair_sigs stored [%sms]", res.modified_count, t)

#-----------------------------------------------------------------------------
def load_db_aggr(title=None):
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
    df = pd.DataFrame(list(get_db().aggr_signals.find()))
    if len(df) == 0:
        return None
    df.index = pd.MultiIndex.from_tuples(list(zip(df.pair, df.freq, df.period)))
    dfa = df[["flip_date", "signal"]]
    dfa.index.names = ["pair", "freq", "period"]
    return dfa

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
