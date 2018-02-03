# Linode host: 45.79.176.125

DEBUGFILE = "logs/debug.log"
LOGFILE = "logs/info.log"
MONGO_PORT = 27017
DB="crypto"
FREQ = 30
CURRENCY = "cad"

# Coinmarketcap.com API
from datetime import datetime
from app.utils import to_int

CMC_MARKETS = [
    {"from":"last_updated", "to":"date", "type":datetime.utcfromtimestamp},
    {"from":"total_market_cap_usd", "to":"mktcap_usd", "type":to_int},
    {"from":"total_24h_volume_usd", "to":"vol_24h_usd", "type":to_int},
    {"from":"bitcoin_percentage_of_market_cap", "to":"pct_mktcap_btc", "type":float},
    {"from":"active_assets", "to":"n_assets", "type":to_int},
    {"from":"active_currencies", "to":"n_currencies", "type":to_int},
    {"from":"active_markets", "to":"n_markets", "type":to_int}
]
CMC_TICKERS = [
    {"from":"id", "to":"id", "type":str},
    {"from":"symbol", "to":"symbol", "type":str},
    {"from":"name", "to":"name", "type":str},
    {"from":"last_updated", "to":"date", "type":datetime.utcfromtimestamp},
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
