# app.coinmktcap
import logging, pycurl, requests, json, time
from io import BytesIO
from .timer import Timer
from config import CMC_MARKETS, CMC_TICKERS, CURRENCY as cur
from app import db

# Silence annoying log msgs
logging.getLogger("requests").setLevel(logging.ERROR)

log = logging.getLogger(__name__)

#---------------------------------------------------------------------------
def get_markets():
    log.info('Requesting CMC markets')
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
        db.coinmktcap_markets.replace_one({'datetime':store['datetime']}, store, upsert=True)

    log.info("Received in %ss" % t1.clock())

#------------------------------------------------------------------------------
def get_tickers(start, limit=None):
    log.info('Requesting CMC tickers')
    chunk_size = 100
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
        db.coinmktcap_tickers.replace_one({'symbol':store['symbol']}, store, upsert=True)

    log.info("%s ticker symbols rec'd in %ss", len(results), t.clock())
