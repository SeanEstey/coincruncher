import logging
from datetime import datetime, timedelta
from dateutil import tz
from dateutil.parser import parse
import pytz
from pprint import pprint
from app import db
log = logging.getLogger(__name__)

from app.history import download_data, extract_data, processDataFrame, parse_options, render_csv_data

#------------------------------------------------------------------------------
def get_ticker_historical():
    currency, start_date, end_date = parse_options("ethereum", "2018-01-01", "2018-01-25")
    html = download_data(currency, start_date, end_date)
    header, rows = extract_data(html)
    #print(rows)

    docrows = []
    # row: ["date", "open", "high", "low", "close", "vol_24h_usd", "mktcap_usd"]
    for row in rows:
        # TODO: add "symbol", "id", and "name"
        docrows.append({
            "date":parse(row[0]).replace(tzinfo=pytz.utc),
            "open":float(row[1]),
            "high":float(row[2]),
            "low":float(row[3]),
            "close":float(row[4]),
            "vol_24h_usd":float(row[5]),
            "mktcap_usd":float(row[6])
        })

    pprint(docrows)

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
