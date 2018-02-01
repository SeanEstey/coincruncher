# app.markets

import logging, pytz
from pprint import pprint
from datetime import datetime, date, timedelta as delta
import pandas as pd
from app import get_db
from app.screen import pretty
from app.utils import parse_period, utc_dt
log = logging.getLogger(__name__) #'app.markets')

#------------------------------------------------------------------------------
def diff(period, to_format):
    """Compare market cap to given date.
    @period: str time period to compare. i.e. '1H', '1D', '7D'
    @to_format: 'currency' or 'percentage'
    """
    db = get_db()
    qty, unit, tdelta = parse_period(period)
    dt = datetime.now(tz=pytz.UTC) - tdelta

    mkts = [
        list(db.market.find({"date":{"$gte":dt}}).sort("date",1).limit(1)),
        list(db.market.find({}).sort("date", -1).limit(1))
    ]

    for m in  mkts:
        if len(m) < 1 or m[0].get('mktcap_cad') is None:
            return 0.0

    mkts[0] = mkts[0][0]
    mkts[1] = mkts[1][0]

    dt_diff = round((mkts[0]['date'] - dt).total_seconds() / 3600, 2)
    if dt_diff > 1:
        log.debug("mktcap lookup fail. period='%s', closest='%s', tdelta='%s hrs'",
        period, mkts[0]['date'].strftime("%m-%d-%Y %H:%M"), dt_diff)
        return "--"

    diff = mkts[1]['mktcap_cad'] - mkts[0]['mktcap_cad']
    pct = round((diff / mkts[0]['mktcap_cad']) * 100, 2)

    return pct if to_format == 'percentage' else diff

#------------------------------------------------------------------------------
def resample(start, end, freq):
    """Resample datetimes from '5M' to given frequency
    @freq: '1H', '1D', '7D'
    """
    db = get_db()
    qty, unit, tdelta = parse_period(freq)
    dt = datetime.now(tz=pytz.UTC) - tdelta

    results = db.market.find(
        {'date':{'$gte':from_dt}},
        {'_id':0,'n_assets':0,'n_currencies':0,'n_markets':0,'pct_mktcap_btc':0})

    df = pd.DataFrame(list(results))
    df.index = df['date']
    del df['date']
    df = df.resample(freq).mean()
    return df

#------------------------------------------------------------------------------
def aggregate_series(start, end):
    """TODO: refactor so this calls aggregate inside loop
    """
    db = get_db()

    dt = start
    while dt <= end:
        # Build market analysis for yesterday's data
        results = db.market.find(
            {"date": {"$gte":dt, "$lt":dt + delta(days=1)}},
            {'_id':0,'n_assets':0,'n_currencies':0,'n_markets':0,'pct_mktcap_btc':0})

        if results.count() < 1:
            log.debug("no datapoints for '%s'", dt)
            dt += delta(days=1)
            continue

        log.debug("resampling %s data points", results.count())

        # Build pandas dataframe and resample to 1D
        df = pd.DataFrame(list(results))
        df.index = df['date']
        df = df.resample("1D").mean()
        df_dict = df.to_dict(orient='records')

        if len(df_dict) != 1:
            log.error("dataframe length is %s!", len(df_dict))
            raise Exception("invalid df length")

        df_dict = df_dict[0]
        df_dict["date"] = dt
        df_dict["mktcap_cad"] = int(df_dict["mktcap_cad"])
        df_dict["mktcap_usd"] = int(df_dict["mktcap_usd"])
        df_dict["vol_24h_cad"] = int(df_dict["vol_24h_cad"])
        df_dict["vol_24h_usd"] = int(df_dict["vol_24h_usd"])
        db.market.historical.insert_one(df_dict)

        log.info("market.historical inserted for '%s'.", dt)

        dt += delta(days=1)

#------------------------------------------------------------------------------
def aggregate():
    # Check if yesterday's market data has been added to market.historical
    # Return n_seconds to end of today for next update.
    db = get_db()
    today = datetime.utcnow().replace(tzinfo=pytz.utc).date()
    yday = utc_dt(today + delta(days=-1))

    if db.market.historical.find({"date":yday}).count() > 0:
        now = datetime.utcnow().replace(tzinfo=pytz.utc)
        tmrw = utc_dt(today + delta(days=1))
        log.debug("market.historical update in %s", tmrw - now)
        return int((tmrw - now).total_seconds())

    # Build market analysis for yesterday's data
    results = db.market.find(
        {"date": {"$gte":yday, "$lt":yday+delta(days=1)}},
        {'_id':0,'n_assets':0,'n_currencies':0,'n_markets':0,'pct_mktcap_btc':0})

    log.debug("resampling %s data points", results.count())

    # Build pandas dataframe and resample to 1D
    df = pd.DataFrame(list(results))
    df.index = df['date']
    df = df.resample("1D").mean()
    df_dict = df.to_dict(orient='records')

    if len(df_dict) != 1:
        log.error("dataframe length is %s!", len(df_dict))
        raise Exception("invalid df length")

    df_dict[0]["date"] = yday
    db.market.historical.insert_one(df_dict[0])

    now = datetime.utcnow().replace(tzinfo=pytz.utc)
    tmrw = utc_dt(today + delta(days=1))

    log.info("market.historical updated for '%s'. next update in %s",
        yday.date(), tmrw - now)

    return int((tmrw - now).total_seconds())

#------------------------------------------------------------------------------
def mcap_avg_diff(freq):
    """@freq: '1H', '1D', '7D'
    """
    caps = list(mktcap_resample(freq)['mktcap_cad'])
    if len(caps) < 2:
        return 0.0
    diff = round(((caps[-1] - caps[-2]) / caps[-2]) * 100, 2)
    return diff

#-------------------------------------------------------------------------------
def update_hist_mkt():
    # Fill in missing historical market data w/ recent data
    pass

#------------------------------------------------------------------------------
def gen_hist_mkts():
    """Initialize market.historical data with aggregate ticker.historical data
    """
    db = get_db()
    results = list(db.tickers.historical.aggregate([
        {"$group": {
          "_id": "$date",
          "mktcap_usd": {"$sum":"$mktcap_usd"},
          "vol_24h_usd": {"$sum":"$vol_24h_usd"},
          "n_symbols": {"$sum":1}
        }},
        {"$sort": {"_id":-1}}
    ]))

    print("generated %s results" % len(results))

    for r in results:
        r.update({'date':r['_id']})
        del r['_id']

    # Remove all documents within date range of aggregate results
    db.market.historical.delete_many({"date":{"$lte":results[0]["date"]}})
    db.market.historical.insert_many(results)
