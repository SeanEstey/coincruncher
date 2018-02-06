# app.tickers

import logging, pytz, json
from datetime import datetime, timedelta, date
from dateutil import tz
from dateutil.parser import parse
from pymongo import ReplaceOne

from app import get_db
from app.timer import Timer
from app.utils import to_float, to_int, parse_period
from app.coinmktcap import download_data, extract_data
log = logging.getLogger('app.tickers')

#------------------------------------------------------------------------------
def update_1d():
    """Update 1d ticker data from 5m data for each ticker.

    If today document doesn't exist, create it: {
        "id":<>,
        "symbol":<>,
        "name":<>,
        "date":<>,
        "close":None,
        "open":<price_usd>,
        "low":<price_usd>,
        "high":<price_usd>,
        "mktcap_usd":<mktcap_usd>, # Start or end of day value?
        "vol_24h_usd":<vol_24h_usd>, # Start or end of day value?
        "rank_now":<rank>
    }

    If today document exists, update: {
        "low":min("$low", price_usd),
        "high":max("$high", price_usd),
        "close":price_usd,
        "mktcap_usd":<mktcap_usd>
        "vol_24h_usd":<vol_24h_usd>
    }
    db = get_db()
    today = utc_date()
    yday_dt = utc_dt(today + delta(days=-1))

    if db.tickers.agg.find({"date":yday_dt}).count() > 0:
        tmrw = utc_tomorrow_delta()
        log.debug("markets.agg update in %s", tmrw)
        return int(tmrw.total_seconds())

    # Build market analysis for yesterday's data
    results = db.markets.find(
        {"date": {"$gte":yday_dt, "$lt":yday_dt+delta(days=1)}},
        {'_id':0,'n_assets':0,'n_currencies':0,'n_markets':0,'pct_mktcap_btc':0})

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

    # Convert numpy types to python and store
    df_dict[0] = numpy_to_py(df_dict[0])
    df_dict[0]["date"] = yday_dt
    db.markets.agg.insert_one(df_dict[0])

    now = datetime.utcnow().replace(tzinfo=pytz.utc)
    tmrw = utc_dt(today + delta(days=1))

    log.info("markets.agg updated for '%s'. next update in %s",
        yday_dt.date(), tmrw - now)

    return int((tmrw - now).total_seconds())
    """
    return True

#------------------------------------------------------------------------------
def diff(symbol, price, period, to_format):
    """Compare current ticker price to historical.
    @price: float in USD
    @offset: str time period to compare. i.e. '1H', '1D', '7D'
    @convert: return diff as percentage (dollar value by default)
    """
    db = get_db()
    qty, unit, tdelta = parse_period(period)
    compare_dt = parse(str(date.today())) - tdelta

    ticker = db.tickers_1d.find({"symbol":symbol, "date":compare_dt})

    if ticker.count() < 1:
        return None

    ticker = list(ticker)[0]

    diff = price - ticker["close"]
    pct = round((diff / ticker["close"]) * 100, 2)

    return pct if to_format == 'percentage' else diff



#------------------------------------------------------------------------------
def update_all_historical():
    db = get_db()

    # TODO: Choose better starting date
    start = parse("2018-01-10")
    end = parse(str(date.today()))

    for ticker in db.tickers_5m.find():
        update_historical(ticker, start, end)

#------------------------------------------------------------------------------
def update_historical(ticker, start, end):
    """Scrape coinmarketcap.com for historical ticker data
    """
    db = get_db()
    bulkops = []
    t1 = Timer()

    # Scrape data
    html = download_data(ticker["id"], start.strftime("%Y%m%d"),
        end.strftime("%Y%m%d"))
    header, rows = extract_data(html)

    for row in rows:
        # ["date", "open", "high", "low", "close", "vol_24h_usd", "mktcap_usd"]
        document = {
            "symbol":ticker["symbol"],
            "id":ticker["id"],
            "name":ticker["name"],
            "date":parse(row[0]).replace(tzinfo=pytz.utc),
            "open":float(row[1]),
            "high":float(row[2]),
            "low":float(row[3]),
            "close":float(row[4]),
            "spread":float(row[2]) - float(row[3]),
            "vol_24h_usd":to_int(row[5]),
            "mktcap_usd":to_int(row[6]),
            "rank_now":ticker["rank"]
        }
        bulkops.append(ReplaceOne(
            {"symbol":ticker["symbol"], "date":document["date"]},
            document,
            upsert=True))

    if len(bulkops) < 1:
        log.info("No results for symbol=%s, start=%s, end=%s",
            ticker["symbol"], start, end)
        return True

    result = db.tickers_1d.bulk_write(bulkops)

    log.info("upd_hist_tckr: sym=%s, scraped=%s, mod=%s, upsert=%s (%s ms)",
        ticker["symbol"], len(rows), result.modified_count, result.upserted_count,
        t1.clock(t='ms'))
