import logging, pytz, json
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
def diff(symbol, price, period, convert=None):
    """Compare current ticker price to historical.
    @price: float in USD
    @offset: str time period to compare. i.e. '1H', '1D', '7D'
    @convert: return diff as percentage (dollar value by default)
    """
    db = get_db()
    unit = period[-1]
    n = int(period[0:-1]) if len(period) > 1 else 1
    today_dt = parse(str(date.today()))

    if unit == 'M':
        dt = today_dt - timedelta(minutes=n)
    elif unit == 'H':
        dt = today_dt - timedelta(hours=n)
    elif unit == 'D':
        dt = today_dt - timedelta(days=n)
    elif unit == 'Y':
        dt = today_dt - timedelta(days=365*n)

    ticker = db.tickers.historical.find({"symbol":symbol, "date":dt})

    if ticker.count() < 1:
        log.debug("no historical ticker found, symbol=%s, period=%s", symbol, period)
        return None

    ticker = list(ticker)[0]

    diff = price - ticker["close"]
    pct = round((diff / ticker["close"]) * 100, 2)

    #log.debug("Mcap %s=%s%s", period, pct, "%")

    return pct if convert == 'pct' else diff

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
