# app.coinmktcap
import logging, requests, json, time
from pymongo import ReplaceOne
from .timer import Timer
from config import CMC_MARKETS, CMC_TICKERS, CURRENCY as cur
from app import db

# Silence annoying log msgs
logging.getLogger("requests").setLevel(logging.ERROR)

log = logging.getLogger(__name__)

#---------------------------------------------------------------------------
def get_markets():
    #log.info('Requesting CMC markets')
    t1 = Timer()

    # Get CoinMarketCap market data
    cmc_data=None
    try:
        r = requests.get("https://api.coinmarketcap.com/v1/global?convert=%s" % cur)
        data = json.loads(r.text)
    except Exception as e:
        log.warning("Error getting CMC market data: %s", str(e))
        return False
    else:
        store = {}
        for m in CMC_MARKETS:
            store[m["to"]] = m["type"]( data[m["from"]] )
        db.market.replace_one({'date':store['date']}, store, upsert=True)

    log.info("Updated market data in %s ms" % t1.clock(t='ms'))

#------------------------------------------------------------------------------
def get_tickers(start, limit=None):
    idx = start
    t = Timer()

    try:
        uri = "https://api.coinmarketcap.com/v1/ticker/?start=%s&limit=%s&convert=%s" %(
            idx, limit or 1500, 'cad')
        response = requests.get(uri)
        results = json.loads(response.text)
    except Exception as e:
        log.exception("Failed to get cmc ticker: %s", str(e))
        return False

    ops = []
    for ticker in results:
        store={}
        ticker['last_updated'] = float(ticker['last_updated']) if ticker.get('last_updated') else None
        for f in CMC_TICKERS:
            try:
                val = ticker[f["from"]]
                store[f["to"]] = f["type"](val) if val else None
            except Exception as e:
                log.exception("%s error in '%s' field: %s", ticker['symbol'], f["from"], str(e))
                continue
        #db.tickers.replace_one({'symbol':store['symbol']}, store, upsert=True)
        ops.append(ReplaceOne({'symbol':store['symbol']}, store, upsert=True))

    result = db.tickers.bulk_write(ops)

    log.info("Updated %s tickers in %s ms", len(results), t.clock(t='ms'))
