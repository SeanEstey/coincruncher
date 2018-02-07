# app.tickers

import logging, pytz, json
from datetime import datetime, timedelta as delta, date
from dateutil import tz
from dateutil.parser import parse
from pymongo import ReplaceOne, UpdateOne
from app import get_db
from app.timer import Timer
from app.utils import utc_datetime, utc_dtdate, utc_date, to_float, to_int, parse_period
from app.coinmktcap import download_data, extract_data

log = logging.getLogger('app.tickers')

#------------------------------------------------------------------------------
def update_1d():
    """Update ticker '1d' documents from latest '5m' data.
    """
    db = get_db()
    today = utc_dtdate()
    operations=[]

    for ticker in db.tickers_5m.find({"date":{"$gte":today}}):
        operations.append(UpdateOne(
            {"symbol":ticker["symbol"], "date":today},
            {
                "$set": {
                    "symbol":ticker["symbol"],
                    "id":ticker["id"],
                    "name":ticker["name"],
                    "date":today,
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

    if len(operations) < 1:
        return 300

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
