import logging, pytz
from datetime import datetime, timedelta
from pprint import pprint
import pandas as pd
from app import get_db #db
from app.screen import pretty
log = logging.getLogger(__name__)



#------------------------------------------------------------------------------
def append_globaldata_hist():
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
def mcap_diff(period, convert=None):
    """Compare market cap to given date.
    @offset: str time period to compare. i.e. '1H', '1D', '7D'
    @convert: return diff as percentage (dollar value by default)
    """
    db = get_db()
    unit = period[-1]
    n = int(period[0:-1]) if len(period) > 1 else 1
    now = datetime.now(tz=pytz.UTC)

    if unit == 'M':
        dt = now - timedelta(minutes=n)
    elif unit == 'H':
        dt = now - timedelta(hours=n)
    elif unit == 'D':
        dt = now - timedelta(days=n)
    elif unit == 'Y':
        dt = now - timedelta(days=365*n)

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
        log.debug("Mcap lookup fail, period=%s, closest=%s, tdelta=%shrs",
        period, mkts[0]['date'].strftime("%m-%d-%Y %H:%M"), dt_diff)
        return "--"

    diff = mkts[1]['mktcap_cad'] - mkts[0]['mktcap_cad']
    pct = round((diff / mkts[0]['mktcap_cad']) * 100, 2)

    #log.debug("Mcap %s=%s%s", period, pct, "%")

    return pct if convert == 'pct' else diff

#------------------------------------------------------------------------------
def mktcap_resample(freq):
    """Resample datetimes from '5M' to given frequency
    @freq: '1H', '1D', '7D'
    """
    db = get_db()
    unit = freq[-1]
    n = int(freq[0:-1]) if len(freq) > 1 else 1

    if unit == 'M':
        from_dt = now - timedelta(minutes=n)
    elif unit == 'H':
        from_dt = now - timedelta(hours=n)
    elif unit == 'D':
        from_dt = now - timedelta(days=n)
    elif unit == 'Y':
        from_dt = now - timedelta(days=365*n)

    results = db.market.find(
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
    caps = list(mktcap_resample(freq)['mktcap_cad'])
    if len(caps) < 2:
        return 0.0
    diff = round(((caps[-1] - caps[-2]) / caps[-2]) * 100, 2)
    return diff
