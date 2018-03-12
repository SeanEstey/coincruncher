# app.signals
import logging
from pymongo import ReplaceOne
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import app
from app import candles
from app.timer import Timer
from app.utils import utc_datetime as now, to_local
from docs.data import BINANCE
log = logging.getLogger('signals')

PSIG_INDEX_NAMES = ['pair','freq','period','stats']
PSIG_COLUMNS = ['close', 'volume', 'buy_vol', 'buy_ratio', 'trades']
PSIG_LVL3_INDEX = ['candle', 'mean', 'std', 'zscore']
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
    dfa = dfa.sort_values('zscore')
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
        #if isinstance(r['age'], datetime):
        #    hrs = round((now()-r['age']).total_seconds()/3600, 1)
        #    r['age'] = str(int(hrs*60))+'m' if hrs < 1 else str(hrs)+'h'

        dfr = dfp.loc[(r['pair'], r['freq'], r['period'])].copy()
        open_time = to_local(dfr.iloc[0]['candle_open'])
        start = to_local(dfr.iloc[0]['hst_start'])
        end = to_local(dfr.iloc[0]['hst_end'])

        log.log(100, r['pair'])
        log.log(100, "{} Candle:    {:%m-%d-%Y %I:%M%p}-{:%I:%M%p}".format(
            FREQ_TO_STR[r['freq']], open_time, open_time + timedelta(seconds=r['freq'])))

        if start.day == end.day:
            log.log(100, "{} Hist:     {:%m-%d-%Y %I:%M%p}-{:%I:%M%p}".format(
                PER_TO_STR[r['period']], start, end))
        else:
            log.log(100, "{} Hist:     {:%m-%d-%Y-%I:%M%p} - {:%m-%d-%Y-%I:%M%p}".format(
                PER_TO_STR[r['period']], start, end))
        log.log(100,'')

        dfr = dfr[["close", "buy_vol", "buy_ratio", "volume", "trades"]]
        dfr[["buy_vol", "volume", "buy_ratio"]] = dfr[["buy_vol", "buy_ratio", "volume"]].astype(float).round(2)

        lines = dfr.to_string(
            col_space=10,
            #float_format=lambda x: "{:,f}".format(x),
            line_width=100).title().split("\n")
        [log.log(100, line) for line in lines]

        log.log(100, '')
        log.log(100, "Mean Zscore: {:+.1f}, Age: :".format(r['zscore'])) #, r['age']))
        log.log(100, '---------------------------------------------------------------------')

#------------------------------------------------------------------------------
def update():
    """Compute pair/aggregate signal data. Binance candle historical averages..
    """
    t = Timer()
    db = app.get_db()
    _1m = timedelta(minutes=1)
    _1h = timedelta(hours=1)
    _1d = timedelta(hours=24)
    dfp = pd.DataFrame()

    # Calculate Zscores for updated datasets.
    for pair in BINANCE["CANDLES"]:
        c5m = candles.last(pair,"5m")
        t5m = [c5m["open_time"] - (5*_1m), c5m["close_time"] - (5*_1m)]
        c1h = candles.last(pair,"1h")
        t1h = [c1h["open_time"] - (1*_1h), c1h["close_time"] - (1*_1h)]
        c1d = candles.last(pair,"1d")
        t1d = [c1d["open_time"] - (1*_1d), c1d["close_time"] - (1*_1d)]

        for n in range(1,4):
            dfp = dfp.append([
                zscore(pair, "5m", str(n*60)+"m", t5m[0]-(n*60*_1m), t5m[1], c5m),
                zscore(pair, "1h", str(n*24)+"h", t1h[0]-(n*24*_1h), t1h[1], c1h),
                zscore(pair, "1d", str(n*7)+"d",  t1d[0]-(n*7*_1d), t1d[1], c1d)
            ])

    # Mean Zscore
    dfa = pd.DataFrame(dfp.xs('zscore', level=3).mean(axis=1), columns=['zscore']
        ).sort_values('zscore')
    # Apply threshold filter (>2 deviations)
    df_filt = dfa[dfa.zscore >= 2]

    # Send alerts, save to DB for tracking
    if len(df_filt) > 0:
        for idx in df_filt.index.values:
            # New/existing alert?
            cursor = db.zscore_alerts.find({"pair":idx[0], "freq":idx[1], "period":idx[2]})

            if cursor.count() > 0:
                log.info("dataset already stored in alerts")
                continue

            df_alert = df_filt.loc[idx]

            log.log(100, "Alert! %s passed Zscore threshold, Zscore is %s", idx[0], df_alert.zscore)

            index_dict = dict(zip(df_filt.index.names, list(df_alert.name)))
            for k in index_dict.keys():
                if type(index_dict[k]) == np.int64:
                    index_dict[k] = int(index_dict[k])

            alert = df_alert.to_dict()
            alert.update(index_dict)
            alert.update({
                'start_time': now(),
                'end_time': False,
                'exchange': 'Binance',
            })
            db.zscore_alerts.insert_one(alert)
            log.log(100, "Inserted alert")
    else:
        log.info("No data passed threshold filters.")

    # Select inverse df_filt, close any active alerts (end_time==False)
    df_inv = dfa[dfa.zscore < 2]
    curs = db.zscore_alerts.find({"end_time":False})

    if curs.count() > 0:
        active_alerts = list(curs)
        indices = list(df_inv.index.values)

        for alert in active_alerts:
            key = (alert['pair'], alert['freq'], alert['period'])

            if key in indices:
                # Kill the alert
                log.info("Killing %s alert", alert['pair'])

                s = df_inv.loc[key]

                db.zscore_alerts.update_one(
                    {"_id":alert['_id']},
                    {"$set":{"end_time":now(), "end_close":0.50, "end_zscore":s['zscore']}}
                )

    save_db(aggr=dfa, pairs=dfp)
    log_max(dfa, dfp)

    log.info("%s signals generated from %s binance pairs. [%ss]",
        len(dfp), len(BINANCE['CANDLES']), t.elapsed(unit='s'))

    return [dfa, dfp]

#-----------------------------------------------------------------------------
def zscore(pair, freq, period, start, end, candle):
    """Compute Z-Score for given candle data within given time period.
    """
    values = []
    df_hst = candles.load_db(pair, freq, start, end=end)
    historic = df_hst.describe()[1::]

    # Compute std/mean/zscore for properties:
    # close/buy_vol/buy_ratio/volume/n_trades
    for i in PSIG_COLUMNS:
        values.append([
            candle[i],
            historic[i]['mean'],
            historic[i]['std'],
            (candle[i] - historic[i]['mean']) / historic[i]['std']
        ])

    df = pd.DataFrame(
        np.array(values).transpose(),
        index = pd.MultiIndex.from_product(
            [[pair], [STR_TO_FREQ[freq]], [STR_TO_PER[period]], PSIG_LVL3_INDEX],
            names=PSIG_INDEX_NAMES
        ),
        columns = PSIG_COLUMNS
    )
    df["close"] = df["close"].astype('float64')
    df["candle_open"] = candle["open_time"]
    df["hst_start"] = df_hst.iloc[0].name
    df["hst_end"] = df_hst.iloc[-1].name
    return df

#-----------------------------------------------------------------------------
def save_db(aggr=None, pairs=None):
    """Given pair signal dataframe, Generate aggregate (sum) signals on each
    index (pair, freq, period, prop), along with time since last sign change,
    t(signal > 0) and t(signal < 0).
    #dfa.index.names = [x.lower() for x in dfa.index.names]
    #dfa.columns = [x.lower() for x in dfa.columns]
    """
    db = app.get_db()

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
            res = db.zscores.bulk_write(ops)
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
            res = db.zscores.bulk_write(ops)
        except Exception as e:
            return log.exception(str(e))
        log.debug("%s aggregate signals saved. [%sms]", res.modified_count, t2)

#-----------------------------------------------------------------------------
def load_aggregate():
    """Load aggregate signal db records.
    Returns:
        multi-index pd.DataFrame
        index levels: [pair, freq, period]
        columns: [signal, age, prev_zscore]
            signal: aggregate (pair,freq,period,indicator) signal
            age: datetime of most recent signal +/- sign flip.
            prev_zscore: last saved signal (for age)
    """
    t1 = Timer()
    c = app.get_db().aggr_signals.find()

    if c.count() == 0:
        return None

    data = list(c)
    lvls = PSIG_INDEX_NAMES[0:3]
    cols = ['zscore', 'age', 'prev_zscore']
    index = pd.MultiIndex.from_arrays(
        [[x[k] for x in data] for k in lvls],
        names=lvls)
    dfa = pd.DataFrame(
        data = np.array([[x[k] for x in data] for k in cols]).transpose(),
        index = index,
        columns = cols
    ).sort_index()
    dfa.zscore = dfa.zscore.astype('float64')
    log.debug("%s aggregate signal docs retrieved. [%sms]", len(dfa), t1)
    return dfa

#-----------------------------------------------------------------------------
def load_pairs():
    """Load pair signal data from DB as multi-index dataframe.
    Returns:
        pair signals dataframe
        index levels: [pair, freq, period, indicator]
        columns: [candle, mean, std, signal]
    """
    t1 = Timer()
    c = app.get_db().zscores.find()

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
