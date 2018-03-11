# app.signals
import logging
from pymongo import ReplaceOne, UpdateOne
from decimal import Decimal as Dec
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from app import get_db
from app.candles import db_get, last
from app.timer import Timer
from app.utils import utc_datetime as now
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
def log_max(dfa, dfp):
    _hdr = "SIGNAL: {:+.1f}, FREQ: {:}, PER: {:}, AGE: {:}"
    dfa = dfa.sort_values('signal')
    max_sigs=[]

    # Given tuples with index levels=[1,2], find max aggregate series
    for idx in [(300,3600), (3600,86400), (86400,604800)]:
        r = {"freq":idx[0], "period":idx[1]}
        series = dfa.xs(r['freq'], level=1).xs(r['period'], level=1).iloc[-1]
        r['pair'] = series.name
        r.update(series.to_dict())
        max_sigs.append(r)

    log.log(100,'')

    for r in max_sigs:
        if isinstance(r['age'], datetime):
            hrs = round((now()-r['age']).total_seconds()/3600, 1)
            r['age'] = str(int(hrs*60))+'m' if hrs < 1 else str(hrs)+'h'

        log.log(100, r['pair'])
        log.log(100, _hdr.format(r['signal'], FREQ_TO_STR[r['freq']],
            PER_TO_STR[r['period']], r['age']))

        dfr = dfp.loc[(r['pair'], r['freq'], r['period'])]

        lines = dfr.to_string(
            col_space=4,
            float_format=lambda x: '%.3f ' % x,
            line_width=100).upper().split("\n")
        lines = [lines[0]] + lines[2:]

        [log.log(100, line) for line in lines]
        log.log(100, '')

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

    dfp = dfp.sort_index()
    dfa = aggregate(dfp)
    save_db(aggr=dfa, pairs=dfp)

    log_max(dfa, dfp)
    log.info("%s signals generated from %s binance pairs. [%ss]",
        len(dfp), len(BINANCE['CANDLES']), t.elapsed(unit='s'))
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
        dfa["prev_signal"] = dfa_prv["signal"]
        dfa["age"] = dfa_prv["age"].replace(np.nan, False)
        dfa["is_flip"] = dfa["signal"].apply(sign) != dfa["prev_signal"].apply(sign)
        dfa.loc[dfa[dfa.is_flip==True].index, 'age'] = now()
        del dfa["is_flip"]
    else:
        dfa["prev_signal"] = None
        dfa["age"] = None

    return dfa

#-----------------------------------------------------------------------------
def save_db(aggr=None, pairs=None):
    """Given pair signal dataframe, Generate aggregate (sum) signals on each
    index (pair, freq, period, prop), along with time since last sign change,
    t(signal > 0) and t(signal < 0).
    #dfa.index.names = [x.lower() for x in dfa.index.names]
    #dfa.columns = [x.lower() for x in dfa.columns]
    """
    db = get_db()

    if pairs is not None:
        t1 = Timer()
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
        log.debug("%s pair signals saved. [%sms]", res.modified_count, t1)

    if aggr is not None:
        t2 = Timer()
        dfa = aggr
        ops=[]

        for idx in dfa.index.values:
            match = dict(zip(dfa.index.names, dfa.loc[idx].name))
            match['period'] = int(match['period'])
            match['freq'] = int(match['freq'])
            record = match.copy()
            record.update(dfa.loc[idx].to_dict())
            ops.append(ReplaceOne(match, record, upsert=True))
        try:
            res = db.aggr_signals.bulk_write(ops)
        except Exception as e:
            return log.exception(str(e))
        log.debug("%s aggregate signals saved. [%sms]", res.modified_count, t2)

#-----------------------------------------------------------------------------
def load_db_aggr(title=None):
    """Load aggregate signal db records.
    Returns:
        multi-index pd.DataFrame
        index levels: [pair, freq, period]
        columns: [signal, age, prev_signal]
            signal: aggregate (pair,freq,period,indicator) signal
            age: datetime of most recent signal +/- sign flip.
            prev_signal: last saved signal (for age)
    """
    t1 = Timer()
    c = get_db().aggr_signals.find()

    if c.count() == 0:
        return None

    data = list(c)
    lvls = LEVELS[0:3]
    cols = ['signal', 'age', 'prev_signal']
    index = pd.MultiIndex.from_arrays(
        [[x[k] for x in data] for k in lvls],
        names=lvls)
    dfa = pd.DataFrame(
        data = np.array([[x[k] for x in data] for k in cols]).transpose(),
        index = index,
        columns = cols
    ).sort_index()
    dfa.signal = dfa.signal.astype('float64')
    log.debug("%s aggregate signal docs retrieved. [%sms]", len(dfa), t1)
    return dfa

#-----------------------------------------------------------------------------
def load_db_pairs():
    """Load pair signal data from DB as multi-index dataframe.
    Returns:
        pair signals dataframe
            index levels: [pair, freq, period, indicator]
            columns: [candle, mean, std, signal]
    """
    t1 = Timer()
    c = get_db().pair_signals.find()

    if c.count() == 0:
        return None

    data = list(c)
    index = pd.MultiIndex.from_arrays(
        [[x[k] for x in data] for k in LEVELS],
        names=LEVELS)
    dfp = pd.DataFrame(
        data = np.array([[x[k] for x in data] for k in COLUMNS]).transpose(),
        index = index,
        columns = COLUMNS
    ).astype('float64').sort_index()

    log.debug("%s pair signal docs retrieved. [%sms]", len(dfp), t1)
    return dfp
