import logging, pytz
from datetime import datetime, timedelta, date
from dateutil import tz
from dateutil.parser import parse
from pymongo import ReplaceOne

from app import get_db
from app.timer import Timer
from app.utils import to_float
from app.coinmktcap import download_data, extract_data
log = logging.getLogger(__name__)

#------------------------------------------------------------------------------
def upd_all_hist_tckr():
    db = get_db()
    start = parse("2018-01-10")
    end = parse(str(date.today()))
    tickers = db.tickers.find()

    for ticker in tickers:
        upd_hist_tckr(ticker, start, end)

#------------------------------------------------------------------------------
def upd_hist_tckr(ticker, start, end):
    """Scrape coinmarketcap.com for historical ticker data
    """
    db = get_db()
    bulkops = []
    t1 = Timer()

    # Scrape data
    html = download_data(ticker["id"], start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
    # row = ["date", "open", "high", "low", "close", "vol_24h_usd", "mktcap_usd"]
    header, rows = extract_data(html)

    for row in rows:
        document = {
            "symbol":ticker["symbol"],
            "id":ticker["id"],
            "name":ticker["name"],
            "date":parse(row[0]).replace(tzinfo=pytz.utc),
            "open":float(row[1]),
            "high":float(row[2]),
            "low":float(row[3]),
            "close":float(row[4]),
            "vol_24h_usd":to_float(row[5]),
            "mktcap_usd":to_float(row[6]),
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

    result = db.tickers.historical.bulk_write(bulkops)

    log.info("upd_hist_tckr: sym=%s, scraped=%s, mod=%s, upsert=%s (%s ms)",
        ticker["symbol"], len(rows), result.modified_count, result.upserted_count,
        t1.clock(t='ms'))

#-------------------------------------------------------------------------------
def update_hist_forex(symbol, start, end):
    """@symbol: fiat currency to to show USD conversion to
    @start, end: datetime objects in UTC
    """
    db = get_db()
    diff = end - start

    for n in range(0,diff.days):
        # TODO: store 'date' and 'CAD' fields in db.forex_historical collection
        dt = start + timedelta(days=1*n)
        print(dt.isoformat())
        uri = "https://api.fixer.io/%s?base=USD&symbols=%s" %(dt.date(),symbol)

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


