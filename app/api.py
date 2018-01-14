# app.display
import logging, pycurl, requests, json, os, subprocess, sys
from pprint import pprint
from io import BytesIO
from .timer import Timer
from config import *
from app import db
log = logging.getLogger(__name__)

#----------------------------------------------------------------------
def get_markets():
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
        log.info("Retrieved market data in %s sec" % t1.clock())

#----------------------------------------------------------------------
def get_tickers(start, limit):
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
        log.info("Retrieved ticker data %s-%s in %s sec",  idx, idx+chunk_size, t1.clock())

    #print("Total time: %s sec" % t.clock())

    for ticker in results:
        result =db['tickers'].replace_one(
            {'symbol':ticker['symbol']},
            ticker,
            upsert=True
        )

    #print("\033[H\033[J")
    #print("Retrieved and stored %s items in %s sec" %(len(dictionary), t1.clock()))
