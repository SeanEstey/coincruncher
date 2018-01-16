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
#CMC_BASE_URL = "https://api.coinmarketcap.com/v1"
#CMC_TICKER_URL = "https://api.coinmarketcap.com/v1/ticker/?"
WCI_API_KEY = "B8BDV74aQIoF5rQYgZNdQ8VBfdgPN0";
WCI_MARKETS_URL = "https://www.worldcoinindex.com/apiservice/getmarkets";
WCI_URI = WCI_MARKETS_URL + "?key=" + WCI_API_KEY + "&fiat=cad";
