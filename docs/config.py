# Signals
Z_IDX_NAMES = ['PAIR', 'FREQ', 'CLOSE_TIME']
#Z_DIMEN = ['CANDLE', 'MEAN', 'STD', 'ZSCORE']
#Z_FACTORS = ['CLOSE', 'OPEN', 'TRADES', 'VOLUME', 'BUY_RATIO']
Z_FACTORS = ['close', 'volume', 'buy_ratio']
DFC_COLUMNS = ['close', 'open', 'trades', 'volume', 'buy_volume']

# App
HOST="45.79.176.125"
MONGO_PORT = 27017
DB_NAME="coincruncher"
DB_DUMP_PATH="~/Dropbox/mongodumps"
DROPBOXD_PATH="/opt/dropbox/dropboxd"

# Logger
DEBUGFILE = "logs/debug.log"
LOGFILE = "logs/info.log"
SIGNALFILE = "logs/signals.log"
SIGNAL = 100
TRADEFILE = "logs/trade.log"
TRADE = 99
ANALYZEFILE = "logs/analyze.log"
ANALYZE = 98
MAX_LOG_DATE_WIDTH=14
MAX_LOG_NAME_WIDTH=8
MAX_LOG_LINE_WIDTH=125
LOG_NEWL_INDENT=25

# Client
DISP_REFRESH_DELAY = 30000
DISP_SCROLL_SP = 5
DISP_PAD_HEIGHT = 200

# API
TICKER_LIMIT=500
CURRENCY = "cad"
