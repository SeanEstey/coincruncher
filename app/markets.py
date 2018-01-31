# app.markets

import logging, pytz
from datetime import datetime, timedelta
import pandas as pd
from app import get_db
from app.screen import pretty
from app.utils import parse_period
log = logging.getLogger('app.markets')

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
def resample(freq):
    # TODO store_hist() generate same result as resample()?
    # Compare performance of aggregate query vs dataframe resample
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
def update_historical():
    # TODO store_hist() generate same result as resample()?
    # Compare performance of aggregate query vs dataframe resample
    db = get_db()
    # Group by date, get closing mktcap/volume
    results = db.market.aggregate([
        {"$match":{}},
        {"$group": {
            # Compound key
            "_id": {
                "day": { "$dayOfYear": "$date" }
                #"hour": { "$hour": "$date" }
            },
            "volume": { "$avg": "$vol_24h_cad"}
        }}
        #"date": { "$max":"$date" },
        # "mktcap": {"$last":"$mktcap_cad"},
        # "vol_24h": {"$last":"$vol_24h_cad"}
    ])
    return results

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
