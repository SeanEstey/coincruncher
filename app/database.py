import logging, pytz
from datetime import datetime, timedelta
from dateutil import tz
from dateutil.parser import parse
from pprint import pprint
from pymongo import ReplaceOne

from app import db, utils
from app.timer import Timer
from app.coinmktcap import download_data, extract_data, processDataFrame, parse_options, render_csv_data
log = logging.getLogger(__name__)

#------------------------------------------------------------------------------
def get_ticker_historical(ticker, start, end, update_db):
    """Scrape coinmarketcap.com for historical ticker data
    """
    t1 = Timer()

    # Scrape data
    html = download_data(ticker["id"], start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
    header, rows = extract_data(html)

    docrows = []
    ins_ops = []

    # row = ["date", "open", "high", "low", "close", "vol_24h_usd", "mktcap_usd"]
    for row in rows:
        _date = parse(row[0]).replace(tzinfo=pytz.utc)
        docrows.append({
            "symbol":ticker["symbol"],
            "id":ticker["id"],
            "name":ticker["name"],
            "date":_date,
            "open":float(row[1]),
            "high":float(row[2]),
            "low":float(row[3]),
            "close":float(row[4]),
            "vol_24h_usd":float(row[5]),
            "mktcap_usd":float(row[6]),
            "rank_now":ticker["rank"]
        })
        if update_db:
            ins_ops.append(ReplaceOne({'symbol':ticker["symbol"], "date":_date}, docrows[-1], upsert=True))

    if len(ins_ops) > 0:
        result = db.tickers.historical.bulk_write(ins_ops)
        print("tickers.hist: symbol=%s, n_scraped=%s, n_modified=%s, \
            n_upserted=%s, time=%s ms" %(
            ticker["symbol"], len(docrows), result.modified_count,
            result.upserted_count, t1.clock(t='ms')))

#------------------------------------------------------------------------------
def generate_historical_markets():
    """Initialize market.historical data with aggregate ticker.historical data
    """
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

#-------------------------------------------------------------------------------
def fill_mkt_historical():
    # Fill in missing historical market data w/ recent data
    pass

#-------------------------------------------------------------------------------
def fill_forex(symbol, start, end):
    """@symbol: fiat currency to to show USD conversion to
    @start, end: datetime objects in UTC
    """
    diff = end - start

    for n in range(0,diff.days):
        # TODO: store 'date' and 'CAD' fields in db.forex_historical collection
        dt = start + timedelta(days=1*n)
        print(dt.isoformat())
        uri = "https://api.fixer.io/%s?base=USD&symbols=%s" %(dt.date(),symbol)
