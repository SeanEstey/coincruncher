# app.markets

import logging
from datetime import datetime, timedelta
from pprint import pformat
import pandas as pd
from app import get_db
from app.utils import duration, utc_dtdate, utc_datetime
log = logging.getLogger('markets')

#---------------------------------------------------------------------------
def next_update():
    """Seconds remaining until next API data refresh.
    """
    updated_dt = list(collection.find().sort("date",-1).limit(1))[0]['date']
    elapsed = int((utc_dt() - updated_dt).total_seconds())

    log.debug("%s sec since '%s' last_updated timestamp",
        collection.name, elapsed.total_seconds())

    if elapsed >= api_refresh:

        return 0
    else:
        assert(elapsed >= 0)
        return api_refresh - elapsed

#---------------------------------------------------------------------------
def db_audit():
    """Verifies the completeness of the db collections, generates any
    necessary documents to fill gaps if possible.
    """
    db = get_db()
    log.debug("DB: verifying...")

    # Verify market_idx_1d completeness
    market_1d = db.market_idx_1d.find().sort('date',-1).limit(1)
    last_date = list(market_1d)[0]["date"]

    n_days = utc_dtdate() - timedelta(days=1) - last_date
    log.debug("DB: market_idx_1d size: {:,}".format(market_1d.count()))

    for n in range(0,n_days.days):
        generate_1d(last_date + timedelta(days=n+1))

#------------------------------------------------------------------------------
def generate_1d(_date):
    """Generate '1d' market index on given date from '5m' data (~236 datapoints).
    Source data is from coinmarketcap API global data.
    """

    db = get_db()

    # Already generated?
    if db.market_idx_1d.find_one({"date":_date}):
        log.debug("marketidx_1d already exists for '%s'", _date.date())
        return 75000

    # Gather source data
    cursor = db.market_idx_5m.find({"date":{"$gte":_date, "$lt":_date+timedelta(days=1)}})

    if cursor.count() < 1:
        log.error("no '5m' source data found on '%s'", _date.date())
        return 75000

    g_data = list(cursor)
    r1 = (g_data[0]["date"], g_data[-1]["date"])

    log.debug("building 1d market_idx, date='%s', t='%s-%s', n_datapoints=%s",
        _date.date(), r1[0].strftime("%H:%M"), r1[1].strftime("%H:%M"), len(g_data))

    # Pandas magic
    df = pd.DataFrame(g_data)
    df.index = df['date']
    del df['_id']
    df_mcap = df.describe().astype(int)["mktcap_usd"]
    db.market_idx_1d.insert_one({
        "date":                   _date,
        "mktcap_open_usd":        int(g_data[0]["mktcap_usd"]),
        "mktcap_close_usd":       int(g_data[-1]["mktcap_usd"]),
        "mktcap_high_usd":        int(df_mcap["max"]),
        "mktcap_low_usd":         int(df_mcap["min"]),
        "mktcap_spread_usd":      int(df_mcap["max"] - df_mcap["min"]),
        "mktcap_mean_24h_usd":    int(df_mcap["mean"]),
        "mktcap_std_24h_usd" :    int(df_mcap["std"]),
        "vol_24h_close_usd":      int(g_data[-1]["vol_24h_usd"]),
    	"n_markets":              g_data[-1]["n_markets"],
		"n_assets":               g_data[-1]["n_assets"],
    	"n_currencies":           g_data[-1]["n_currencies"],
		"n_symbols":              g_data[-1]["n_currencies"] + g_data[-1]["n_assets"],
    	"btc_mcap":               g_data[-1]["pct_mktcap_btc"]
    })

    log.info("market_idx_1d generated.")
    #log.debug(pformat(db.market_idx_1d.find_one({"date":_date})))

    return 75000

#------------------------------------------------------------------------------
def update_1d_series(start, end):
    """TODO: refactor so this calls aggregate inside loop
    """
    db = get_db()

    dt = start
    while dt <= end:
        # Build market analysis for yesterday's data
        results = db.market_idx_5m.find(
            {"date": {"$gte":dt, "$lt":dt + timedelta(days=1)}},
            {'_id':0,'n_assets':0,'n_currencies':0,'n_markets':0,'pct_mktcap_btc':0})

        if results.count() < 1:
            log.debug("no datapoints for '%s'", dt)
            dt += timedelta(days=1)
            continue

        log.debug("resampling %s data points", results.count())

        # Build pandas dataframe and resample to 1D
        df = pd.DataFrame(list(results))
        df.index = df['date']
        df = df.resample("1D").mean()
        cols = ["mktcap_usd", "vol_24h_usd"]
        df[cols] = df[cols].fillna(0.0).astype(int)
        df_dict = df.to_dict(orient='records')

        if len(df_dict) != 1:
            log.error("dataframe length is %s!", len(df_dict))
            raise Exception("invalid df length")

        df_dict = df_dict[0]
        df_dict["date"] = dt
        db.market_idx_1d.insert_one(df_dict)

        log.info("market_idx_1d inserted for '%s'.", dt)

        dt += timedelta(days=1)

#------------------------------------------------------------------------------
def diff(period, to_format):
    """Compare market cap to given date.
    @period: str time period to compare. i.e. '1H', '1D', '7D'
    @to_format: 'currency' or 'percentage'
    """
    import pytz
    from app.utils import parse_period
    db = get_db()
    qty, unit, tdelta = parse_period(period)
    dt = datetime.now(tz=pytz.UTC) - tdelta

    mkts = [
        list(db.market_idx_5m.find({"date":{"$gte":dt}}).sort("date",1).limit(1)),
        list(db.market_idx_5m.find({}).sort("date", -1).limit(1))
    ]

    for m in  mkts:
        if len(m) < 1 or m[0].get('mktcap_usd') is None:
            return 0.0

    mkts[0] = mkts[0][0]
    mkts[1] = mkts[1][0]

    dt_diff = round((mkts[0]['date'] - dt).total_seconds() / 3600, 2)
    if dt_diff > 1:
        log.debug("mktcap lookup fail. period='%s', closest='%s', tdelta='%s hrs'",
        period, mkts[0]['date'].strftime("%m-%d-%Y %H:%M"), dt_diff)
        return "--"

    diff = mkts[1]['mktcap_usd'] - mkts[0]['mktcap_usd']
    pct = round((diff / mkts[0]['mktcap_usd']) * 100, 2)

    return pct if to_format == 'percentage' else diff

#------------------------------------------------------------------------------
def resample(start, end, freq):
    """Resample datetimes from '5M' to given frequency
    @freq: '1H', '1D', '7D'
    """
    import pytz
    from app.utils import parse_period
    db = get_db()
    qty, unit, tdelta = parse_period(freq)
    dt = datetime.now(tz=pytz.UTC) - tdelta

    results = db.market_idx_5m.find(
        {'date':{'$gte':from_dt}},
        {'_id':0,'n_assets':0,'n_currencies':0,'n_markets':0,'pct_mktcap_btc':0})

    df = pd.DataFrame(list(results))
    df.index = df['date']
    del df['date']
    df = df.resample(freq).mean()
    return df

#------------------------------------------------------------------------------
def mcap_avg_diff(freq):
    """@freq: '1H', '1D', '7D'
    """
    caps = list(mktcap_resample(freq)['mktcap_usd'])
    if len(caps) < 2:
        return 0.0
    diff = round(((caps[-1] - caps[-2]) / caps[-2]) * 100, 2)
    return diff
