DEBUGFILE = "logs/debug.log"
LOGFILE = "logs/info.log"
MONGO_URL = 'localhost'
MONGO_PORT = 27017
LOCAL_PORT = 8000
LOCAL_URL = 'http://localhost:%s' % LOCAL_PORT
DB = 'crypto'
FREQ = 30
CURRENCY = "cad"
INPUT_TIMEOUT = 0.1 # seconds

# Worldcoinindex.com API
WCI_API_KEY = "B8BDV74aQIoF5rQYgZNdQ8VBfdgPN0";
WCI_MARKETS_URL = "https://www.worldcoinindex.com/apiservice/getmarkets";
WCI_URI = WCI_MARKETS_URL + "?key=" + WCI_API_KEY + "&fiat=cad";

# Coinmarketcap.com API
from datetime import datetime
CMC_MARKETS = [
    {"from":"last_updated", "to":"datetime", "type":datetime.fromtimestamp},
    {"from":"total_market_cap_cad", "to":"mktcap_cad", "type":float},
    {"from":"total_24h_volume_cad", "to":"vol_24h_cad", "type":float},
    {"from":"bitcoin_percentage_of_market_cap", "to":"pct_mktcap_btc", "type":float},
    {"from":"active_assets", "to":"n_assets", "type":int},
    {"from":"active_currencies", "to":"n_currencies", "type":int},
    {"from":"active_markets", "to":"n_markets", "type":int}
]
CMC_TICKERS = [
    {"from":"id", "to":"id", "type":str},
    {"from":"symbol", "to":"symbol", "type":str},
    {"from":"name", "to":"name", "type":str},
    {"from":"last_updated", "to":"datetime", "type":datetime.fromtimestamp},
    {"from":"rank", "to":"rank", "type":int},
    {"from":"market_cap_cad", "to":"mktcap_cad", "type":float},
    {"from":"24h_volume_cad", "to":"vol_24h_cad", "type":float},
    {"from":"price_cad", "to":"price_cad", "type":float},
    {"from":"percent_change_1h", "to":"pct_1h", "type":float},
    {"from":"percent_change_24h", "to":"pct_24h", "type":float},
    {"from":"percent_change_7d", "to":"pct_7d", "type":float},
    {"from":"available_supply", "to":"avail_supply", "type":float},
    {"from":"total_supply", "to":"total_supply", "type":float},
    {"from":"max_supply", "to":"max_supply", "type":float}
]

# Coincap.io API
"""COINCAP_MARKETS = {
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
