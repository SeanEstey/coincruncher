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
from docs.config import FREQ_TO_STR as freqtostr
from docs.config import PER_TO_STR as pertostr
log = logging.getLogger('signals')

_1m = timedelta(minutes=1)
_1h = timedelta(hours=1)
_1d = timedelta(hours=24)
PSIG_LVL3_INDEX = ['candle', 'mean', 'std', 'zscore']
PSIG_INDEX_NAMES = ['pair','freq','period','stats']
PSIG_COLUMNS = ['close', 'volume', 'buy_vol', 'buy_ratio', 'trades']
strtofreq = dict(zip(list(freqtostr.values()), list(freqtostr.keys())))
strtoper = dict(zip(list(pertostr.values()), list(pertostr.keys())))

#------------------------------------------------------------------------------
def update():
    """Compute pair/aggregate signal data. Binance candle historical averages..
    """
    t = Timer()
    db = app.get_db()
    # Hist.avg vs candle stat.analysis
    df_h = pd.DataFrame()

    # Calculate z-scores for updated datasets
    for pair in BINANCE["CANDLES"]:
        c5m = candles.last(pair,"5m")
        t5m = [c5m["open_time"] - (5*_1m), c5m["close_time"] - (5*_1m)]
        c1h = candles.last(pair,"1h")
        t1h = [c1h["open_time"] - (1*_1h), c1h["close_time"] - (1*_1h)]
        c1d = candles.last(pair,"1d")
        t1d = [c1d["open_time"] - (1*_1d), c1d["close_time"] - (1*_1d)]

        for n in range(1,4):
            df_h = df_h.append([
                zscore(pair, "5m", str(n*60)+"m", t5m[0]-(n*60*_1m), t5m[1], c5m),
                zscore(pair, "1h", str(n*24)+"h", t1h[0]-(n*24*_1h), t1h[1], c1h),
                zscore(pair, "1d", str(n*7)+"d",  t1d[0]-(n*7*_1d), t1d[1], c1d)
            ])

    log.info("%s z-scores generated from binance data. [%ss]",
        len(df_h), t.elapsed(unit='s'))

    save_db(df_h)

    # Mean Zscore
    df_z = pd.DataFrame(df_h.xs('zscore', level=3).mean(axis=1),
        columns=['zscore']
    ).sort_values('zscore')

    # Create alerts for any abnormal spikes in trading
    scan_thresholds(df_h)

    # Print actionable trading signals
    show_alerts(df_h, df_z)


    return (df_h, df_z)

#-----------------------------------------------------------------------------
def zscore(pair, freq, period, start, end, candle):
    """Measure deviation from the mean for this candle data across the given
    historical time period. Separate z-scores are assigned for each candle
    property.
        Z-Score of 1: within standard deviation
        Z-Score of 2.5: large deviation from mean, sign of trading pattern
        diverging sharply from current historical pattern.
    """
    data = []
    # Save as int to enable df index sorting
    _freq = strtofreq[freq]
    _period = strtoper[period]
    columns = ['close', 'volume', 'buy_vol', 'buy_ratio', 'trades']
    stats_idx = ['candle', 'mean', 'std', 'zscore']

    # Statistical data and z-score
    hist_data = candles.load_db(pair, freq, start, end=end)
    hist_avg = hist_data.describe()[1::]

    for x in columns:
        data.append([
            candle[x],
            hist_avg[x]['mean'],
            hist_avg[x]['std'],
            (candle[x] - hist_avg[x]['mean']) / hist_avg[x]['std']
        ])

    index = pd.MultiIndex.from_product(
        [[pair],[_freq],[_period], stats_idx],
        names=['pair','freq','period','stats']
    )

    # Reverse lists. x-axis: candle_fields, y-axis: stat_data
    df_h = pd.DataFrame(np.array(data).transpose(),
        index=index,
        columns=columns
    )

    # Enhance floating point precision for small numbers
    df_h["close"] = df_h["close"].astype('float64')

    df_h["open_time"] = candle["open_time"]
    df_h["hist_start"] = hist_data.iloc[0].name
    df_h["hist_end"] = hist_data.iloc[-1].name

    return df_h

#-----------------------------------------------------------------------------
def save_db(df_h):
    """Given pair signal dataframe, Generate aggregate (sum) signals on each
    index (pair, freq, period, prop), along with time since last sign change,
    t(signal > 0) and t(signal < 0).
    #df_z.index.names = [x.lower() for x in df_z.index.names]
    #df_z.columns = [x.lower() for x in df_z.columns]
    """
    db = app.get_db()
    t1 = Timer()
    ops=[]

    for idx in df_h.index.values:
        match = dict(zip(df_h.index.names, df_h.loc[idx].name))
        match['period'] = int(match['period'])
        match['freq'] = int(match['freq'])
        record = match.copy()
        record.update(df_h.loc[idx].to_dict())
        ops.append(ReplaceOne(match, record, upsert=True))
    try:
        res = db.historical_avg.bulk_write(ops)
    except Exception as e:
        return log.exception(str(e))
    log.debug("%s pair signals saved. [%sms]", res.modified_count, t1)

    t2 = Timer()
    ops=[]

    """
    for idx in df_z.index.values:
        match = dict(zip(df_z.index.names, df_z.loc[idx].name))
        match['period'] = int(match['period'])
        match['freq'] = int(match['freq'])
        record = match.copy()
        record.update(df_z.loc[idx].to_dict())
        ops.append(ReplaceOne(match, record, upsert=True))
    try:
        res = db.zscores.bulk_write(ops)
    except Exception as e:
        return log.exception(str(e))
    log.debug("%s aggregate signals saved. [%sms]", res.modified_count, t2)
    """

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
    df_h = pd.DataFrame(
        data = np.array([[x[k] for x in data] for k in COLUMNS]).transpose(),
        index = index,
        columns = COLUMNS
    ).astype('float64').sort_index()

    log.debug("%s pair signal docs retrieved. [%sms]", len(df_h), t1)
    return df_h

#------------------------------------------------------------------------------
def show_alerts(df_h, df_z):
    """Print alerts to signal log.
    """
    df_z = df_z.sort_values('zscore')
    max_sigs=[]

    # Given tuples with index levels=[1,2], find max aggregate series
    for idx in [(300,3600), (3600,86400), (86400,604800)]:
        r = {"freq":idx[0], "period":idx[1]}
        series = df_z.xs(r['freq'], level=1).xs(r['period'], level=1).iloc[-1]
        r['pair'] = series.name
        r.update(series.to_dict())
        max_sigs.append(r)

    log.log(100,'')

    for r in max_sigs:
        #if isinstance(r['age'], datetime):
        #    hrs = round((now()-r['age']).total_seconds()/3600, 1)
        #    r['age'] = str(int(hrs*60))+'m' if hrs < 1 else str(hrs)+'h'

        dfr = df_h.loc[(r['pair'], r['freq'], r['period'])].copy()
        open_time = to_local(dfr.iloc[0]['open_time'])
        start = to_local(dfr.iloc[0]['hist_start'])
        end = to_local(dfr.iloc[0]['hist_end'])

        log.log(100, r['pair'])
        log.log(100, "{} Candle:    {:%m-%d-%Y %I:%M%p}-{:%I:%M%p}".format(
            freqtostr[r['freq']], open_time, open_time + timedelta(seconds=r['freq'])))

        if start.day == end.day:
            log.log(100, "{} Hist:     {:%m-%d-%Y %I:%M%p}-{:%I:%M%p}".format(
                pertostr[r['period']], start, end))
        else:
            log.log(100, "{} Hist:     {:%m-%d-%Y-%I:%M%p} - {:%m-%d-%Y-%I:%M%p}".format(
                pertostr[r['period']], start, end))
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
        log.log(100, '-'*80)

#------------------------------------------------------------------------------
def scan_thresholds(df_h):
    """Check if any updated datasets have z-scores > 2, track in DB to determine
    price correlations.
    """
    from pprint import pprint, pformat
    db = app.get_db()

    # Mean Zscore
    df_z = pd.DataFrame(df_h.xs('zscore', level=3).mean(axis=1),
        columns=['zscore']).sort_values('zscore')

    # Apply threshold filter (>2 deviations)
    df_filt = df_z[df_z.zscore >= 2]

    # Send alerts, save to DB for tracking
    if len(df_filt) > 0:
        for idx in df_filt.index.values:
            index_dict = {"pair":idx[0], "freq":idx[1], "period":idx[2]}
            cursor = db.zscores.find(index_dict)

            if cursor.count() > 0:
                # Update existing
                continue

            # Insert new record
            df_hist = df_h.loc[idx]
            hist_dict = df_hist[df_hist.columns[0:5]].to_dict()
            hist_dict.update({
                'open_time': df_h.iloc[0]['open_time'],
                'hist_start': df_h.iloc[0]['hist_start'],
                'hist_end': df_h.iloc[0]['hist_end']
            })
            record = index_dict
            record.update({
                'start_time': now(),
                'end_time': False,
                'exchange': 'Binance',
                'history': hist_dict,
                'zscore': df_filt.loc[idx].zscore,
                'start_price': hist_dict['close']['candle']
            })
            db.zscores.insert_one(record)

    # Select inverse df_filt. Close any active alerts (end_time==False)
    df_inv = df_z[df_z.zscore < 2]
    curs = db.zscores.find({"end_time":False})

    if curs.count() > 0:
        active_alerts = list(curs)
        indices = list(df_inv.index.values)

        for alert in active_alerts:
            key = (alert['pair'], alert['freq'], alert['period'])

            if key in indices:
                end_price = float(df_h.loc[key]['close']['candle'])
                pct_change = (end_price - alert['start_price'])/alert['start_price']*100

                log.debug('Closing %s alert. Closing price=%s, pct_change=%s',
                    alert['pair'], end_price, pct_change)

                index = df_inv.loc[key]

                db.zscores.update_one(
                    {"_id": alert['_id']},
                    {"$set": {
                        'end_time': now(),
                        'end_price': float(df_h.loc[key]['close']['candle']),
                        'pct_change': pct_change,
                        'end_zscore': index['zscore']
                    }}
                )
