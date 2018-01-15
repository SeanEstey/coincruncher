# app.display
import logging, pycurl, requests, json, os, subprocess, sys
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
    data=None
    try:
        r = requests.get("https://api.coinmarketcap.com/v1/global?convert=%s" % CURRENCY)
        data = json.loads(r.text)
    except Exception as e:
        print("Error getting market data: %s" % str(e))
        return False
    else:
        data['currency'] = CURRENCY
        db['markets'].replace_one(
            {'last_updated':data['last_updated']},
            data,
            upsert=True
        )
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
