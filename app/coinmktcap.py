# app.coinmktcap
from datetime import datetime
import argparse, logging, requests, json, pytz, re, sys, time
from sys import getsizeof as getsize
from pymongo import ReplaceOne
from .timer import Timer
from config import CMC_MARKETS, CMC_TICKERS, CURRENCY as cur
from app import get_db

log = logging.getLogger('app.coinmktcap')
# Silence annoying log msgs
#logging.getLogger("requests").setLevel(logging.ERROR)
parser = argparse.ArgumentParser()
parser.add_argument("currency", help="", type=str)
parser.add_argument("start_date",  help="", type=str)
parser.add_argument("end_date", help="", type=str)
parser.add_argument("--dataframe", help="", action='store_true')

#---------------------------------------------------------------------------
def update():
    """Query and store coinmarketcap market/ticker data. Sync data fetch with
    CMC 5 min update frequency.
    """
    # Fetch coinmarketcap data every 5 min
    CMC_UPDT_FREQ = 300
    # Update daily ticker historical data at end of each day.
    # Use closing price
    UPDT_HIST_TCKR_FREQ = 3600 * 24

    db = get_db()
    updated_dt = list(db.market.find().sort('_id',-1).limit(1))[0]['date']
    now = datetime.utcnow().replace(tzinfo=pytz.utc)
    t_remain = int(CMC_UPDT_FREQ - (now - updated_dt).total_seconds())

    if t_remain <= 0:
        updt_tickers(0,1500)
        updt_markets()
        log.debug("data refresh in %s sec.", CMC_UPDT_FREQ)
        time.sleep(60)
    else:
        log.debug("data refresh in %s sec.", t_remain)
        time.sleep(min(t_remain, 60))

#---------------------------------------------------------------------------
def updt_markets():
    """Get CoinMarketCap global market data
    """
    data=None
    store={}
    t1 = Timer()
    db = get_db()
    log.info("fetching market data...")

    try:
        response = requests.get("https://api.coinmarketcap.com/v1/global?convert=%s" % cur)
        data = json.loads(response.text)
    except Exception as e:
        log.warning("Error getting CMC market data: %s", str(e))
        return False
    else:
        for m in CMC_MARKETS:
            store[m["to"]] = m["type"]( data[m["from"]] )

        db.market.replace_one({'date':store['date']}, store, upsert=True)

    log.info("received %s bytes in %s ms.", getsize(response.text), t1.clock(t='ms'))

#------------------------------------------------------------------------------
def updt_tickers(start, limit=None):
    """Get CoinMarketCap ticker data for all assets.
    """
    idx = start
    t = Timer()
    db = get_db()
    log.info("fetching ticker data...")

    try:
        response = requests.get(
            "https://api.coinmarketcap.com/v1/ticker/?start={}&limit={}&convert={}"\
            .format(idx, limit or 1500, 'cad'))
        data = json.loads(response.text)
    except Exception as e:
        log.exception("updt_ticker() error")
        return False

    ops = []

    for ticker in data:
        store={}
        ticker['last_updated'] = float(ticker['last_updated']) if \
            ticker.get('last_updated') else None

        for f in CMC_TICKERS:
            try:
                val = ticker[f["from"]]
                store[f["to"]] = f["type"](val) if val else None
            except Exception as e:
                log.exception("%s error in '%s' field: %s",
                    ticker['symbol'], f["from"], str(e))
                continue

        ops.append(ReplaceOne({'symbol':store['symbol']}, store, upsert=True))

    result = db.tickers.bulk_write(ops)

    log.info("received {:,} bytes in {:,} ms. {:,} tickers.".format(
        getsize(response.text), t.clock(t='ms'), len(data)))

#---------------------------------------------------------------------------
def parse_options(currency, start_date, end_date):
  """Extract parameters from command line.
  """
  currency   = currency.lower()
  #start_date = args.start_date
  #end_date   = args.end_date

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
    print('Usage: ' + __file__ + ' <currency> <start_date> <end_date> --dataframe')
    sys.exit(1)

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
    page = requests.get(url) #,timeout=10)
    if page.status_code != 200:
      print(page.status_code)
      print(page.text)
      raise Exception('Failed to load page')
    html = page.text

  except Exception as e:
    print('Error fetching price data from ' + url)
    print('Did you use a valid CoinMarketCap currency?\nIt should be entered exactly as displayed on CoinMarketCap.com (case-insensitive), with dashes in place of spaces.')
    print(str(e))

    if hasattr(e, 'message'):
      print("Error message: " + e.message)
    else:
      print(e)
      sys.exit(1)

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
