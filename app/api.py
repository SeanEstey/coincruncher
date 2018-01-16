# app.display
import logging, pycurl, requests, json, time
from pprint import pprint
from io import BytesIO
from .timer import Timer
from config import *
from app import db
log = logging.getLogger(__name__)

# Silence annoying requests module logger
logging.getLogger("urllib3").setLevel(logging.WARNING)

#----------------------------------------------------------------------
def setup_db(collection, data):
    # Initialize if collection empty
    if db[collection].find().count() == 0:
        for item in data:
            db[collection].insert_one(item)
            log.info('Initialized %s symbol %s', collection, doc['symbol'])
    # Update collection
    else:
        for item in data:
            db[collection].replace_one({'symbol':item['symbol']}, item, upsert=True)
            log.debug('Updated %s symbol %s', collection, item['symbol'])

        symbols = [ n['symbol'] for n in data ]
        for doc in db[collection].find():
            if doc['symbol'] not in symbols:
                log.debug('Deleted %s symbol %s', collection, doc['symbol'])
                db[collection].delete_one({'_id':doc['_id']})

    log.info("DB updated w/ user data")

#----------------------------------------------------------------------
def update_markets():
    t1 = Timer()

    # Get CoinMarketCap market data
    cmc_data=None
    try:
        r = requests.get("https://api.coinmarketcap.com/v1/global?convert=%s" % CURRENCY)
        cmc_data = json.loads(r.text)
    except Exception as e:
        print("Error getting market data: %s" % str(e))
        pass
    else:
        cmc_data['currency'] = CURRENCY
        db['markets'].replace_one(
            {'last_updated':cmc_data['last_updated']},
            cmc_data,
            upsert=True
        )

    """{
        base: "USD",
        date: "2018-01-15",
        rates: {
            CAD: 1.2432
        }
    }
    """
    try:
        r = requests.get("https://api.fixer.io/latest?base=USD&symbols=CAD")
        usd_cad = json.loads(r.text)["rates"]["CAD"]
    except Exception as e:
        log.warning("Error getting USD/CAD rate")
        pass

    # Get Coincap.io market data
    """{
        altCap: 504531918898.49554,
        bitnodesCount: 11680,
        btcCap: 241967814774,
        btcPrice: 14402,
        dom: 65.6,
        totalCap: 746499733672.4971,
        volumeAlt: 1651343165.0478735,
        volumeBtc: 3148874332.6655655,
        volumeTotal: 4800217497.713445
    }
    """
    cc_data=None
    try:
        r = requests.get("http://coincap.io/global", headers={'Cache-Control': 'no-cache'})
        cc_data = json.loads(r.text)
    except Exception as e:
        log.warning("Error getting Coincap.io market data")
        pass
    else:
        cc_data['timestamp'] = int(time.time())
        #for n in ['altCap','btcCap','totalCap']:
        #    cc_data[n] *= usd_cad
        db.coincap_global.insert_one(cc_data)

    log.info("Received in %ss" % t1.clock())

#----------------------------------------------------------------------
def update_tickers(start, limit):
    chunk_size = 100
    idx = start
    results = []
    t = Timer()
    c = pycurl.Curl()
    c.setopt(c.COOKIEFILE, '')
    #c.setopt(c.VERBOSE, True)

    while idx < limit:
        t1 = Timer()
        uri = "https://api.coinmarketcap.com/v1/ticker/?start=%s&limit=%s&convert=%s" %(idx, chunk_size, CURRENCY)
        data=BytesIO()
        c.setopt(c.WRITEFUNCTION, data.write)
        c.setopt(c.URL, uri)
        c.perform()
        results += json.loads(data.getvalue().decode('utf-8'))
        idx += chunk_size
        log.debug("Retrieved ticker data %s-%s in %s sec",  idx, idx+chunk_size, t1.clock())

    log.info("Received %s items in %ss", len(results), t.clock())

    for ticker in results:
        result =db['tickers'].replace_one(
            {'symbol':ticker['symbol']},
            ticker,
            upsert=True
        )
