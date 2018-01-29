DEBUGFILE = "logs/debug.log"
LOGFILE = "logs/info.log"
MONGO_URL = "45.79.176.125"
MONGO_PORT = 27017
DB = "crypto"
FREQ = 30
CURRENCY = "cad"
INPUT_TIMEOUT = 0.1 # seconds

# Coinmarketcap.com API
from datetime import datetime
CMC_MARKETS = [
    {"from":"last_updated", "to":"date", "type":datetime.utcfromtimestamp},
    {"from":"total_market_cap_usd", "to":"mktcap_usd", "type":float},
    {"from":"total_market_cap_cad", "to":"mktcap_cad", "type":float},
    {"from":"total_24h_volume_usd", "to":"vol_24h_usd", "type":float},
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
    {"from":"last_updated", "to":"date", "type":datetime.utcfromtimestamp},
    {"from":"rank", "to":"rank", "type":int},
    {"from":"market_cap_usd", "to":"mktcap_usd", "type":float},
    {"from":"market_cap_cad", "to":"mktcap_cad", "type":float},
    {"from":"24h_volume_usd", "to":"vol_24h_usd", "type":float},
    {"from":"24h_volume_cad", "to":"vol_24h_cad", "type":float},
    {"from":"price_usd", "to":"price_usd", "type":float},
    {"from":"price_cad", "to":"price_cad", "type":float},
    {"from":"percent_change_1h", "to":"pct_1h", "type":float},
    {"from":"percent_change_24h", "to":"pct_24h", "type":float},
    {"from":"percent_change_7d", "to":"pct_7d", "type":float},
    {"from":"available_supply", "to":"circulating_supply", "type":float},
    {"from":"total_supply", "to":"total_supply", "type":float},
    {"from":"max_supply", "to":"max_supply", "type":float}
]
