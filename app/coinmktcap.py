# app.coinmktcap
import logging, pycurl, requests, json, time
from io import BytesIO
from .timer import Timer
from config import CMC_MARKETS, CMC_TICKERS, CURRENCY as cur
from app import db
log = logging.getLogger(__name__)

# Silence annoying requests module logger
logging.getLogger("urllib3").setLevel(logging.WARNING)

#---------------------------------------------------------------------------
def get_markets():
    t1 = Timer()

    # Get CoinMarketCap market data
    cmc_data=None
    try:
        r = requests.get("https://api.coinmarketcap.com/v1/global?convert=%s" % cur)
    except Exception as e:
        log.warning("Error getting CMC market data: %S", str(e))
        pass
    else:
        data = json.loads(r.text)
        store = {}
        for m in CMC_MARKETS:
            store[m["to"]] = m["type"]( data[m["from"]] )
        db.markets.replace_one({'datetime':store['datetime']}, store, upsert=True)

    log.info("Received in %ss" % t1.clock())

#---------------------------------------------------------------------------
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
        uri = "https://api.coinmarketcap.com/v1/ticker/?start=%s&limit=%s&convert=%s" %(
            idx, chunk_size, 'cad')
        data=BytesIO()
        c.setopt(c.WRITEFUNCTION, data.write)
        c.setopt(c.URL, uri)
        c.perform()
        results += json.loads(data.getvalue().decode('utf-8'))
        idx += chunk_size

    log.info("%s ticker symbols rec'd in %ss", len(results), t.clock())

    for ticker in results:
        store={}
        ticker['last_updated'] = float(ticker['last_updated'])
        for f in CMC_TICKERS:
            try:
                val = ticker[f["from"]]
                store[f["to"]] = f["type"](val) if val else None
            except Exception as e:
                log.exception("Error in field=%s, val=%s", f["from"], ticker[f["from"]])
                continue
        db.tickers.replace_one({'symbol':store['symbol']}, store, upsert=True)
