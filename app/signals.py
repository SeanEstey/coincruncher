# app.signals
import logging
from pymongo import ReplaceOne, UpdateOne
from decimal import Decimal as Dec
from datetime import timedelta
import pandas as pd
import numpy as np
from app import get_db
from app.candles import db_get, last
from app.timer import Timer
from app.utils import utc_datetime as now, to_float, parse_period as per_to_sec
from docs.data import BINANCE
log = logging.getLogger('signals')

# Names for each index level
LEVELS = ['pair','freq','period','indicator']
# Names for index level 3
INDICATORS = ['close', 'volume', 'buy_vol', 'buy_ratio', 'trades']
COLUMNS = ['candle', 'mean', 'std', 'signal']
FREQ_TO_STR = {
    300: "5m",
    3600: "1h",
    86400: "1d"
}
PER_TO_STR = {
    3600: "60m",
    7200: "120m",
    10800: "180m",
    86400: "24h",
    172800: "48h",
    259200: "72h",
    604800: "7d",
    1209600: "14d",
    1814400: "21d"
}
STR_TO_FREQ = dict(zip(
    list(FREQ_TO_STR.values()), list(FREQ_TO_STR.keys())))
STR_TO_PER = dict(zip(
    list(PER_TO_STR.values()), list(PER_TO_STR.keys())))

#------------------------------------------------------------------------------
def siglog(dfa, dfp):
    from pprint import pformat
    log.log(100,'')

    # Iterate over index level 1 (5m, 1h, 1d)
    # Find highest pair signals for each
    for freq in list(dfa.index.levels[1]):
        df = dfa.xs(freq, level=1).sort_values('signal', ascending=False)
        pair = df.iloc[0].name[0]
        period = df.iloc[0].name[1]
        dfpmax = dfp.loc[(pair, freq, period)]

        #log.debug(pformat(dfpmax, width=90))

        dfpmax_idx = dfpmax.signal.idxmax() # Index level 3
        signal = dfpmax.loc[dfpmax_idx]['signal']
        close = dfp.loc[(pair, freq, period, "close")]
        close_cndl = close['candle']
        close_mean = close['mean']
        close_pct = ((close_cndl - close_mean)/close_mean)*100
        flip_d =  df.iloc[0].flip_date
        duration = round((now()-flip_d).total_seconds()/3600, 1) if flip_d else 0.0

        log.log(100,
            "Max({0},{1}): {2}, ".format(
                FREQ_TO_STR[freq], PER_TO_STR[period], pair) +\
            "Signal: {:+.1f}, ".format(df.signal.max()) +\
            "Age: {:.1f}h, ".format(duration) +\
            "{0} x STD: {1}, ".format(
                dfpmax_idx.title().replace('_',''), round(signal, 1)) +\
            "Close vs Mean: {0:+.2f}%".format(close_pct)
        )

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
    siglog(dfa, dfp)

    log.info("Binance signals updated. n=%s, t=%sms", len(dfp)+len(dfa), t)
    return [dfa, dfp]

#-----------------------------------------------------------------------------
def calculate(pair, freq, period, start, end):
    """Generate signal strength values for candle indicators by measuring
    number of standard deviations from historical mean.

    ADDME:
        # Price diff in past t=freq
        close_diff = dfc["close"] - dfh.iloc[-1].close
    """
    values = []
    candle = db_get(pair, freq, None).to_dict('record')[0]
    df_hst = db_get(pair, freq, start, end=end)
    hst_avg = df_hst.describe()[1::]

    # 'close', 'volume', 'buy_vol', 'buy_ratio', 'trades'
    for i in INDICATORS:
        values.append([
            candle[i],
            hst_avg[i]['mean'],
            hst_avg[i]['std'],
            (candle[i] - hst_avg[i]['mean']) / hst_avg[i]['std']
        ])

    return pd.DataFrame(
        values,
        index = pd.MultiIndex.from_product(
            [[pair], [STR_TO_FREQ[freq]], [STR_TO_PER[period]], INDICATORS],
            names=LEVELS
        ),
        columns = COLUMNS
    ).astype(np.float64)

#-----------------------------------------------------------------------------
def aggregate(dfp):
    t = Timer()
    db = get_db()
    sign = lambda x: -1 if x<0 else 1 if x>0 else 0

    dfa = pd.DataFrame(dfp.groupby(level=[0,1,2]).sum()["signal"])
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

    if pairs is not None:
        dfp = pairs
        ops=[]

        for idx in dfp.index.values:
            match = dict(zip(dfp.index.names, dfp.loc[idx].name))
            match['period'] = int(match['period'])
            match['freq'] = int(match['freq'])
            record = match.copy()
            record.update(dfp.loc[idx].to_dict())
            ops.append(ReplaceOne(match, record, upsert=True))
        try:
            res = db.pair_signals.bulk_write(ops)
        except Exception as e:
            return log.exception(str(e))
        return log.debug("%s pair_sigs stored [%sms]", res.modified_count, t)

    if aggr is not None:
        dfa = aggr
        ops=[]

        for idx in dfa.index.values:
            ops.append(ReplaceOne(
                dict(zip(LEVELS[0:3], idx)), {"$set":dfa.loc[idx].to_dict()},
                upsert=True))
        try:
            res = db.aggr_signals.bulk_write(ops)
        except Exception as e:
            return log.exception(str(e))
        log.debug("%s aggr_sigs stored [%sms]", res.modified_count, t)

#-----------------------------------------------------------------------------
def load_db_aggr(title=None):
    """Load aggregate signal data from DB as multi-index dataframe.
    Returns:
        aggregate signals dataframe
            index levels: [pair, freq, period]
            columns: [signal, flip_date]
    """
    df = pd.DataFrame(list(get_db().aggr_signals.find()))
    if len(df) == 0:
        return None
    df.index = pd.MultiIndex.from_tuples(list(zip(df.pair, df.freq, df.period)))
    dfa = df[["flip_date", "signal"]]
    dfa.index.names = LEVELS[0:3]
    return dfa

#-----------------------------------------------------------------------------
def load_db_pairs():
    """Load pair signal data from DB as multi-index dataframe.
    Returns:
        pair signals dataframe
            index levels: [pair, freq, period, indicator]
            columns: [candle, mean, std, signal]
    """
    df = pd.DataFrame(list(get_db().pair_signals.find()))
    if len(df) == 0:
        return None
    df.index = pd.MultiIndex.from_tuples(list(zip(df.pair, df.freq, df.period, INDICATORS)))
    dfa.index.names = LEVELS
    return dfa
