# App
HOST="45.79.176.125"
MONGO_PORT = 27017
DB="crypto"
DB_DUMP_PATH="~/Dropbox/mongodumps"
DROPBOXD_PATH="/opt/dropbox/dropboxd"

# Logger
DEBUGFILE = "logs/debug.log"
LOGFILE = "logs/info.log"
MAX_LOG_DATE_WIDTH=14
MAX_LOG_NAME_WIDTH=8
MAX_LOG_LINE_WIDTH=75
LOG_NEWL_INDENT=25

# API
TICKER_LIMIT=500
FREQ = 30
CURRENCY = "cad"
BINANCE_PAIRS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "LTCUSDT",
    "BCCUSDT",
    "EOSBTC",
    "NANOBTC",
    "NANOETH",
    "NEOBTC",
    "OMGBTC",
    "BLZBTC",
    "ETCBTC",
    "XMRBTC",
    "NEBLBTC"
]

# Client
DISP_REFRESH_DELAY = 30000
DISP_SCROLL_SP = 5
DISP_PAD_HEIGHT = 200
