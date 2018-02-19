# app.tickers

import logging, pytz, json
from pprint import pformat, pprint
from datetime import datetime, timedelta as delta, date
from dateutil import tz
from dateutil.parser import parse
from pymongo import ReplaceOne, UpdateOne
import pandas as pd
from app import get_db
from app.timer import Timer
from app.utils import utc_datetime, utc_dtdate, utc_date, to_float, to_int, parse_period
from app.coinmktcap import download_data, extract_data
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
log = logging.getLogger('tickers')

#------------------------------------------------------------------------------
def db_audit():
    # Verify tickers_1d completeness
    db = get_db()

    _tickers = db.tickers_1d.aggregate([
        {"$group":{
            "_id":"$id",
            "date":{"$max":"$date"},
            "name":{"$last":"$name"},
            "symbol":{"$last":"$symbol"},
            "rank":{"$last":"$rank_now"},
            "count":{"$sum":1}
        }},
        {"$sort":{"rank":1}}
    ])
    _tickers = list(_tickers)

    log.debug("%s aggregated ticker_1d assets", len(_tickers))

    for tckr in _tickers:
        last_update = utc_dtdate() - delta(days=1) - tckr["date"]
        if last_update.total_seconds() < 1:
            log.debug("%s up-to-date.", tckr["symbol"])
            continue

        log.debug("updating %s (%s out-of-date)", tckr["symbol"], last_update)

        get_history(
            tckr["_id"],
            tckr["name"],
            tckr["symbol"],
            tckr["rank"],
            tckr["date"],
            utc_dtdate())

    log.debug("DB: verified")

#------------------------------------------------------------------------------
def get_history(_id, name, symbol, rank, start, end):
    """Scrape coinmarketcap for historical ticker data in given date range.
    """
    db = get_db()
    bulkops = []
    t1 = Timer()

    # Scrape data
    try:
        html = download_data(_id, start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
    except Exception as e:
        return log.exception("Error scraping %s", symbol)
    else:
        header, rows = extract_data(html)

    for row in rows:
        # ["date", "open", "high", "low", "close", "vol_24h_usd", "mktcap_usd"]
        document = {
            "symbol":symbol,
            "id":_id,
            "name":name,
            "date":parse(row[0]).replace(tzinfo=pytz.utc),
            "open":float(row[1]),
            "high":float(row[2]),
            "low":float(row[3]),
            "close":float(row[4]),
            "spread":float(row[2]) - float(row[3]),
            "vol_24h_usd":to_int(row[5]),
            "mktcap_usd":to_int(row[6]),
            "rank_now":rank
        }
        bulkops.append(ReplaceOne(
            {"symbol":symbol, "date":document["date"]},
            document,
            upsert=True))

    if len(bulkops) < 1:
        log.info("No results for symbol=%s, start=%s, end=%s",
            symbol, start, end)
        return True

    result = db.tickers_1d.bulk_write(bulkops)

    log.info("upd_hist_tckr: sym=%s, scraped=%s, mod=%s, upsert=%s (%s ms)",
        symbol, len(rows), result.modified_count, result.upserted_count, t1)

#------------------------------------------------------------------------------
def generate_1d(_date):
    """Generate '1d' ticker data from stored '5m' datapoints. Alternative to
    scraping data off coinmarketcap.
    """
    db = get_db()

    # Already generated?
    if db.tickers_1d.find_one({"date":_date}):
        log.debug("tickers_1d already exists for '%s'", _date.date())
        return 75000

    # Gather source data
    cursor = db.tickers_5m.find({"date":{"$gte":_date, "$lt":_date+delta(days=1)}})

    if cursor.count() < 1:
        log.error("no '5m' source data found on '%s'", _date.date())
        return 75000

    operations = []

    for ticker in cursor:
        operations.append(UpdateOne(
            {"symbol":ticker["symbol"], "date":_date},
            {
                "$set": {
                    "symbol":ticker["symbol"],
                    "id":ticker["id"],
                    "name":ticker["name"],
                    "date":_date,
                    "close":ticker["price_usd"],
                    "mktcap_usd":ticker["mktcap_usd"],
                    "vol_24h_usd":ticker["vol_24h_usd"],
                    "rank_now":ticker["rank"]
                },
                "$setOnInsert":{"open":ticker["price_usd"]},
                "$max":{"high":ticker["price_usd"]},
                "$min":{"low":ticker["price_usd"]},
            },
            upsert=True))

    log.debug("tickers_1d writing %s updates...", len(operations))

    try:
        result = db.tickers_1d.bulk_write(operations)
    except Exception as e:
        log.exception("update_1d bulk_write error")
        return 300

    log.info("tickers_1d updated. %s modified, %s upserted.",
        result.modified_count, result.upserted_count)

    return 300

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
