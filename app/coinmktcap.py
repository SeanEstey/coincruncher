# app.coinmktcap
from datetime import datetime, timedelta
import argparse, logging, requests, json, pytz, re, sys, time
from sys import getsizeof as getsize
from pymongo import ReplaceOne
from .timer import Timer
from docs.config import CURRENCY as cur
from app import get_db
from app.utils import utc_datetime, duration, to_int, to_dt
log = logging.getLogger('global')

parser = argparse.ArgumentParser()
parser.add_argument("currency", help="", type=str)
parser.add_argument("start_date",  help="", type=str)
parser.add_argument("end_date", help="", type=str)
parser.add_argument("--dataframe", help="", action='store_true')

market_f = [
    {"from":"last_updated", "to":"date", "type":to_dt},
    {"from":"total_market_cap_usd", "to":"mktcap_usd", "type":to_int},
    {"from":"total_24h_volume_usd", "to":"vol_24h_usd", "type":to_int},
    {"from":"bitcoin_percentage_of_market_cap", "to":"pct_mktcap_btc", "type":float},
    {"from":"active_assets", "to":"n_assets", "type":to_int},
    {"from":"active_currencies", "to":"n_currencies", "type":to_int},
    {"from":"active_markets", "to":"n_markets", "type":to_int}
]
ticker_f = [
    {"from":"id", "to":"id", "type":str},
    {"from":"symbol", "to":"symbol", "type":str},
    {"from":"name", "to":"name", "type":str},
    #{"from":"last_updated", "to":"date", "type":to_dt},
    {"from":"rank", "to":"rank", "type":to_int},
    {"from":"market_cap_usd", "to":"mktcap_usd", "type":to_int},
    {"from":"24h_volume_usd", "to":"vol_24h_usd", "type":to_int},
    {"from":"available_supply", "to":"circulating_supply", "type":to_int},
    {"from":"total_supply", "to":"total_supply", "type":to_int},
    {"from":"max_supply", "to":"max_supply", "type":to_int},
    {"from":"price_usd", "to":"price_usd", "type":float},
    {"from":"percent_change_1h", "to":"pct_1h", "type":float},
    {"from":"percent_change_24h", "to":"pct_24h", "type":float},
    {"from":"percent_change_7d", "to":"pct_7d", "type":float}
]

#---------------------------------------------------------------------------
def next_update(collection):
    """Seconds remaining until next API data refresh.
    """
    api_refresh = 400
    result = collection.find().sort("date",-1).limit(1)

    if result.count() < 1:
        log.debug("%s empty, refresh now", collection.name)
        return 0

    elapsed = duration(utc_datetime() - list(result)[0]["date"])

    if elapsed >= api_refresh:
        return 0
    else:
        #log.debug("update in %ss", api_refresh-elapsed)
        assert(elapsed >= 0)
        return api_refresh - elapsed + 30

#------------------------------------------------------------------------------
def get_tickers_5t(start=0, limit=None):
    """Update 5T ticker data from coinmarketcap.com REST API.
    """
    _t = next_update(get_db().tickers_5t)
    if _t > 0:
        return _t

    idx = start
    t = Timer()
    db = get_db()
    log.debug("querying tickers")

    r = requests.get("https://api.coinmarketcap.com/v1/ticker/?start={}&limit={}"\
        .format(idx, limit or 0))
    if r.status_code != 200:
        return log.error("API error %s", r.status_code)

    #log.debug("{:,} bytes received ({:,}ms)".format(getsize(r.text), t))
    data = json.loads(r.text)

    # Sort by timestamp in descending order
    data = sorted(data, key=lambda x: int(x["last_updated"] or 1))[::-1]

    # Prune outdated tickers
    ts_range = range(
        int(data[0]["last_updated"]) - 180,
        int(data[0]["last_updated"]) + 1)
    tickerdata = [ n for n in data if n["last_updated"] and int(n["last_updated"]) in ts_range ]
    _dt = to_dt(int(data[0]["last_updated"]))
    updated = _dt - timedelta(seconds=_dt.second, microseconds=_dt.microsecond)
    ops = []

    for ticker in tickerdata:
        store={"date":updated}

        for f in ticker_f:
            try:
                val = ticker[f["from"]]
                store[f["to"]] = f["type"](val) if val else None
            except Exception as e:
                log.exception("%s ticker error", ticker["symbol"])
                continue

        ops.append(ReplaceOne(
            {'date':updated, 'symbol':store['symbol']}, store, upsert=True))

    t2 = Timer()
    result = db.tickers_5t.bulk_write(ops).bulk_api_result
    #log.debug("bulk_write completed (%sms)", t2)
    del result["upserted"]
    #log.debug(result)
    if result["nUpserted"] > 0:
        log.info("Coinmktcap tickers updated [n=%s, freq='5m', t=%sms]", len(tickerdata), t)

#---------------------------------------------------------------------------
def get_marketidx_5t():
    """Update 5T market index data from coinmarketcap.com REST API.
    """
    _t = next_update(get_db().market_idx_5t)
    if _t > 0:
        return _t

    t1 = Timer()
    #log.debug("querying global market")

    r = requests.get("https://api.coinmarketcap.com/v1/global")
    if r.status_code != 200:
        return log.error("%s request error.", r.status_code)

    #log.debug("%s bytes received (%sms)", getsize(r.text), t1)

    data = json.loads(r.text)
    store = {}
    for m in market_f:
        store[m["to"]] = m["type"]( data[m["from"]] )

    get_db().market_idx_5t.replace_one(
        {'date':store['date']}, store,
        upsert=True)

    log.info("Coinmktcap markets updated [n=%s, freq='5m', t=%sms]",
        len(data), t1)

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
