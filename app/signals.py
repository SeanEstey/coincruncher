# app.signals
import logging
from pymongo import ReplaceOne
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import app
from app import candles, trades
from app.timer import Timer
from app.utils import utc_datetime as now, to_local
from docs.data import BINANCE, FREQ_TO_STR as freqtostr, PER_TO_STR as pertostr

log = logging.getLogger('signals')
strtofreq = dict(zip(list(freqtostr.values()), list(freqtostr.keys())))
strtoper = dict(zip(list(pertostr.values()), list(pertostr.keys())))
def siglog(msg): log.log(100, msg)

#------------------------------------------------------------------------------
def update():
    """Compute pair/aggregate signal data. Binance candle historical averages..
    """
    from docs.config import ZSCORE_THRESH
    t1 = Timer()

    # Aggregate zscores for new candle data
    df_z = generate_dataset()
    save_db(df_z)

    df_wa = weighted_average(df_z)

    # Apply threshold filters (n_deviations)
    df_out = df_wa[df_wa.wascore > ZSCORE_THRESH]
    df_in = df_wa[df_wa.wascore <= ZSCORE_THRESH]
    log.info("%s above threshold, %s below.", len(df_out), len(df_in))

    # Send alerts, update trading positions

    for idx, row in df_out.iterrows():
        df = df_z.loc[idx]
        #show_alert(idx, row)
        trades.update_position(idx, row.wascore, df)

    for idx, row in df_in.iterrows():
        df = df_z.loc[idx]
        trades.close_position(idx, row.wascore, df)

    trades.summarize()
    log.info('Scores/trades updated. [%ss]', t1)
    return (df_out, df_wa)

#------------------------------------------------------------------------------
def generate_dataset():
    """Build dataframe with scores for 5m, 1h, 1d frequencies across various
    time periods.
    """
    t1 = Timer()
    _1m = timedelta(minutes=1)
    _1h = timedelta(hours=1)
    _1d = timedelta(hours=24)
    df_data = pd.DataFrame()

    # Calculate z-scores for various (pair, freq, period) keys
    for pair in BINANCE["CANDLES"]:
        c5m = candles.last(pair,"5m")
        t5m = [c5m["open_time"] - (5*_1m), c5m["close_time"] - (5*_1m)]
        c1h = candles.last(pair,"1h")
        t1h = [c1h["open_time"] - (1*_1h), c1h["close_time"] - (1*_1h)]
        c1d = candles.last(pair,"1d")
        t1d = [c1d["open_time"] - (1*_1d), c1d["close_time"] - (1*_1d)]

        for n in range(1,4):
            df_data = df_data.append([
                zscore(pair, "5m", str(n*60)+"m", t5m[0]-(n*60*_1m), t5m[1], c5m),
                zscore(pair, "1h", str(n*24)+"h", t1h[0]-(n*24*_1h), t1h[1], c1h),
                zscore(pair, "1d", str(n*7)+"d",  t1d[0]-(n*7*_1d), t1d[1], c1d)
            ])

    log.info("Generated [%s rows x %s cols] z-score dataframe from Binance candles. [%ss]",
        len(df_data), len(df_data.columns), t1.elapsed(unit='s'))
    return df_data

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

    df_h.metadata_candle_open_time = candle['open_time']
    df_h.metadata_candle_close_time = candle['close_time']
    df_h.metadata_period_start = hist_data.iloc[0].name
    df_h.metadata_period_end = hist_data.iloc[-1].name
    return df_h

#-----------------------------------------------------------------------------
def save_db(df_z):
    """Given pair signal dataframe, Generate aggregate (sum) signals on each
    index (pair, freq, period, prop), along with time since last sign change,
    t(signal > 0) and t(signal < 0).
    """
    db = app.get_db()
    t1 = Timer()
    ops=[]

    for idx, row in df_z.iterrows():
        query = dict(zip(['pair', 'freq', 'period'], idx))
        record = query.copy()
        record.update(row.to_dict())
        ops.append(ReplaceOne(query, record, upsert=True))
    try:
        res = db.zscores.bulk_write(ops)
    except Exception as e:
        return log.exception(str(e))
    log.debug("%s pair signals saved. [%sms]", res.modified_count, t1)

#-----------------------------------------------------------------------------
def load_scores(pair, freq, period):
    """Load pair signal data from DB as multi-index dataframe.
    Returns:
        pair signals dataframe
        index levels: [pair, freq, period, indicator]
        columns: [candle, mean, std, signal]
    """
    curs = app.get_db().zscores.find(
        {"pair":pair, "freq":freq, "period":period})

    if curs.count() == 0:
        return None

    return pd.DataFrame(list(curs),
        index = pd.Index(['candle', 'mean', 'std', 'zscore'], name='stats'),
        columns = ['close', 'volume', 'buy_vol', 'buy_ratio', 'trades']
    ).astype('float64')

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

    log.log(100, '-'*80)

    for r in max_sigs:
        if r['zscore'] < 2:
            continue

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
            log.log(100, "{} Hist:     {:%m-%d-%Y %I:%M%p} - {:%m-%d-%Y %I:%M%p}".format(
                pertostr[r['period']], start, end))
        log.log(100,'')

        dfr = dfr[["close", "buy_vol", "buy_ratio", "volume", "trades"]]
        dfr[["buy_vol", "buy_ratio", "volume"]] = dfr[["buy_vol", "buy_ratio", "volume"]].astype(float).round(2)

        lines = dfr.to_string(
            col_space=10,
            #float_format=lambda x: "{:,f}".format(x),
            line_width=100).title().split("\n")
        [log.log(100, line) for line in lines]

        log.log(100, '')
        log.log(100, "Mean Zscore: {:+.1f}, Age: :".format(r['zscore'])) #, r['age']))
        log.log(100, '-'*80)

#------------------------------------------------------------------------------
def weighted_average(df_z):
    """Check if any updated datasets have z-scores > 2, track in DB to determine
    price correlations.
    """
    from docs.config import ZSCORE_THRESH

    # Weighted average scores
    df_wa = pd.DataFrame(df_z.xs('zscore', level=3).mean(axis=1),
        columns=['wascore']).sort_values('wascore')

    return df_wa


