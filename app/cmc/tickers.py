# app.coinmktcap.tickers
import logging, pytz, json
import argparse, logging, requests, json, re
from datetime import datetime, timedelta as delta, date
import dateparser
from pymongo import ReplaceOne
from app import get_db
from app.common.timer import Timer
from app.common.utils import utc_dtdate, to_int, parse_period, to_dt
from docs.conf import coinmarketcap

log = logging.getLogger('cmc.tickers')
#logging.getLogger("requests").setLevel(logging.ERROR)
#logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
parser = argparse.ArgumentParser()
parser.add_argument("currency", help="", type=str)
parser.add_argument("start_date",  help="", type=str)
parser.add_argument("end_date", help="", type=str)
parser.add_argument("--dataframe", help="", action='store_true')


#------------------------------------------------------------------------------
def update(start=0, limit=None):
    query_api_tick(start=start, limit=limit)
    query_api_mkt()

#------------------------------------------------------------------------------
def query_api_tick(start=0, limit=None):
    """Update 5T ticker data from coinmarketcap.com REST API.
    """
    idx = start
    t1 = Timer()
    db = get_db()

    try:
        r = requests.get("https://api.coinmarketcap.com/v1/ticker/?start={}&limit={}"\
            .format(idx, limit or 0))
        data = json.loads(r.text)
    except Exception as e:
        return log.error("API error %s", r.status_code)

    if r.status_code != 200:
        return log.error("API error %s", r.status_code)

    # Sort by timestamp in descending order
    data = sorted(data, key=lambda x: int(x["last_updated"] or 1))[::-1]

    # Prune outdated tickers
    ts_range = range(
        int(data[0]["last_updated"]) - 180,
        int(data[0]["last_updated"]) + 1)
    tickerdata = [ n for n in data if n["last_updated"] and int(n["last_updated"]) in ts_range ]
    _dt = to_dt(int(data[0]["last_updated"]))
    updated = _dt - delta(seconds=_dt.second, microseconds=_dt.microsecond)
    ops = []

    for ticker in tickerdata:
        store={"date":updated}

        for f in coinmarketcap['api']['tickers']:
            try:
                val = ticker[f["from"]]
                store[f["to"]] = f["type"](val) if val else None
            except Exception as e:
                log.exception("%s ticker error", ticker["symbol"])
                continue

        ops.append(ReplaceOne(
            {'date':updated, 'symbol':store['symbol']}, store, upsert=True))

    if save_capped_db(ops, db.cmc_tick):
        log.info("%s Coinmktcap tickers updated. [%sms]", len(tickerdata), t1)

#---------------------------------------------------------------------------
def query_api_mkt():
    """Update 5T market index data from coinmarketcap.com REST API.
    """
    t1 = Timer()

    try:
        r = requests.get("https://api.coinmarketcap.com/v1/global")
        data = json.loads(r.text)
    except Exception as e:
        return log.error("API error %s", r.status_code)

    if r.status_code != 200:
        return log.error("API error %s", r.status_code)

    print(r.status_code)

    store = {}
    for m in coinmarketcap['api']['markets']:
        store[m["to"]] = m["type"]( data[m["from"]] )

    get_db().cmc_mkt.replace_one(
        {'date':store['date']}, store,
        upsert=True)

    log.info("Coinmktcap markets updated. [{}ms]".format(t1))

#---------------------------------------------------------------------------
def parse_options(currency, start_date, end_date):
    """Extract parameters from command line.
    @currency: full name of asset ("ethereum")
    @start_date, @end_date: date strings
    """
    currency   = currency.lower()
    start_date_split = start_date.split('-')
    end_date_split   = end_date.split('-')
    start_year = int(start_date_split[0])
    end_year   = int(end_date_split[0])
    # String validation
    pattern    = re.compile('[2][0][1][0-9]-[0-1][0-9]-[0-3][0-9]')

    if not re.match(pattern, start_date):
        raise ValueError('Invalid format for the start_date: ' +\
            start_date + ". Should be of the form: yyyy-mm-dd.")
    if not re.match(pattern, end_date):
        raise ValueError('Invalid format for the end_date: '   + end_date  +\
            ". Should be of the form: yyyy-mm-dd.")

    # Datetime validation for the correctness of the date.
    # Will throw a ValueError if not valid
    datetime(start_year,int(start_date_split[1]),int(start_date_split[2]))
    datetime(end_year,  int(end_date_split[1]),  int(end_date_split[2]))

    # CoinMarketCap's price data (at least for Bitcoin, presuambly for all others)
    # only goes back to 2013
    invalid_args =                 start_year < 2013
    invalid_args = invalid_args or end_year   < 2013
    invalid_args = invalid_args or end_year   < start_year

    if invalid_args:
        return print('Usage: ' + __file__ + ' <currency> <start_date> <end_date> --dataframe')

    start_date = start_date_split[0]+ start_date_split[1] + start_date_split[2]
    end_date   = end_date_split[0]  + end_date_split[1]   + end_date_split[2]

    return currency, start_date, end_date

#---------------------------------------------------------------------------
def download_data(currency, start_date, end_date):
    """Download HTML price history for the specified cryptocurrency and time
    range from CoinMarketCap.
    """
    url = 'https://coinmarketcap.com/currencies/' + currency + '/historical-data/' + '?start=' \
        + start_date + '&end=' + end_date
    try:
        page = requests.get(url)
        if page.status_code != 200:
            print(page.status_code)
            print(page.text)
            raise Exception('Failed to load page')
        html = page.text
    except Exception as e:
        print('Error fetching price data from ' + url)
        print(str(e))

    if hasattr(e, 'message'):
        print("Error message: " + e.message)
    else:
        print(e)

    raise Exception("Error scraping data for %s" % currency)
    return html

#---------------------------------------------------------------------------
def extract_data(html):
    """Extract the price history from the HTML.
    The CoinMarketCap historical data page has just one HTML table. This table
    contains the data we want. It's got one header row with the column names.
    We need to derive the "average" price for the provided data.
    """
    head = re.search(r'<thead>(.*)</thead>', html, re.DOTALL).group(1)
    header = re.findall(r'<th .*>([\w ]+)</th>', head)
    header.append('Average (High + Low / 2)')

    body = re.search(r'<tbody>(.*)</tbody>', html, re.DOTALL).group(1)
    raw_rows = re.findall(r'<tr[^>]*>' + r'\s*<td[^>]*>([^<]+)</td>'*7 + r'\s*</tr>', body)

    # strip commas
    rows = []
    for row in raw_rows:
        row = [ field.replace(",","") for field in row ]
        rows.append(row)

    # calculate averages
    def append_average(row):
        high = float(row[header.index('High')])
        low = float(row[header.index('Low')])
        average = (high + low) / 2
        row.append( '{:.2f}'.format(average) )
        return row

    rows = [ append_average(row) for row in rows ]
    return header, rows

#---------------------------------------------------------------------------
def render_csv_data(header, rows):
    """Render the data in CSV format.
    """
    print(','.join(header))
    for row in rows:
        print(','.join(row))

#---------------------------------------------------------------------------
def processDataFrame(df):
    import pandas as pd
    assert isinstance(df, pd.DataFrame), "df is not a pandas DataFrame."

    cols = list(df.columns.values)
    cols.remove('Date')
    df.loc[:,'Date'] = pd.to_datetime(df.Date)
    for col in cols: df.loc[:,col] = df[col].apply(lambda x: float(x))
    return df.sort_values(by='Date').reset_index(drop=True)

#---------------------------------------------------------------------------
def rowsFromFile(filename):
    import csv
    with open(filename, 'rb') as infile:
        rows = csv.reader(infile, delimiter=',')
        for row in rows:
            print(row)

#---------------------------------------------------------------------------
def save_capped_db(ops, coll):
    try:
        result = coll.bulk_write(ops)
    except Exception as e:
        log.exception("Error saving CMC tickers. %s", str(e))
        db = get_db()
        stats = db.command("collstats",coll.name)

        if stats['capped'] == False:
            return False

        max_size = stats['maxSize']

        # Capped collection full. Drop and re-create w/ indexes.
        if stats['size'] / max_size > 0.9:
            from pymongo import IndexModel, ASCENDING, DESCENDING

            log.info("Capped collection > 90% full. Dropping and recreating.")
            name = coll.name
            coll.drop()

            db.create_collection(name, capped=True, size=max_size)
            idx1 = IndexModel( [("symbol", ASCENDING)], name="symbol")
            idx2 = IndexModel( [("date", DESCENDING)], name="date_-1")
            db[name].create_indexes([idx1, idx2])

            log.info("Retrying bulk_write")
            try:
                result = db[name].bulk_write(ops)
            except Exception as e:
                log.exception("Error saving CMC tickers. %s", str(e))
                return False
        else:
            log.error("Size is <90% max. Unknown error.")
    return True

#------------------------------------------------------------------------------
def scrape_history(_id, name, symbol, rank, start, end):
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
            "date":dateparser.parse(row[0]).replace(tzinfo=pytz.utc),
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
def tkr_diff(symbol, price, period, to_format):
    """Compare current ticker price to historical.
    @price: float in USD
    @offset: str time period to compare. i.e. '1H', '1D', '7D'
    @convert: return diff as percentage (dollar value by default)
    """
    db = get_db()
    qty, unit, tdelta = parse_period(period)
    compare_dt = dateparser.parse(str(date.today())) - tdelta
    ticker = db.tickers_1d.find({"symbol":symbol, "date":compare_dt})

    if ticker.count() < 1:
        return None

    ticker = list(ticker)[0]
    diff = price - ticker["close"]
    pct = round((diff / ticker["close"]) * 100, 2)
    return pct if to_format == 'percentage' else diff

#------------------------------------------------------------------------------
def mkt_diff(period, to_format):
    """Compare market cap to given date.
    @period: str time period to compare. i.e. '1H', '1D', '7D'
    @to_format: 'currency' or 'percentage'
    """
    db = get_db()
    qty, unit, tdelta = parse_period(period)
    dt = datetime.now(tz=pytz.UTC) - tdelta

    mkts = [
        list(db.cmc_mkt.find({"date":{"$gte":dt}}).sort("date",1).limit(1)),
        list(db.cmc_mkt.find({}).sort("date", -1).limit(1))
    ]

    for m in  mkts:
        if len(m) < 1 or m[0].get('mktcap_usd') is None:
            return 0.0

    mkts[0] = mkts[0][0]
    mkts[1] = mkts[1][0]

    dt_diff = round((mkts[0]['date'] - dt).total_seconds() / 3600, 2)
    if dt_diff > 1:
        log.debug("mktcap lookup fail. period='%s', closest='%s', tdelta='%s hrs'",
        period, mkts[0]['date'].strftime("%m-%d-%Y %H:%M"), dt_diff)
        return "--"

    diff = mkts[1]['mktcap_usd'] - mkts[0]['mktcap_usd']
    pct = round((diff / mkts[0]['mktcap_usd']) * 100, 2)

    return pct if to_format == 'percentage' else diff

#------------------------------------------------------------------------------
def volatile_24h():
    cursor = get_db().cmc_tick.find({"rank":{"$lte":500}}).sort("date",-1).limit(500)
    tckrs = list(cursor)
    descend = sorted(tckrs, key=lambda x: float(x["pct_24h"] or 0.0), reverse=True)
    return descend[0:5] + descend[::-1][0:5]
