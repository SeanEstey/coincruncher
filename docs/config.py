# Signals
Z_IDX_NAMES = ['PAIR', 'FREQ', 'CLOSE_TIME']
Z_FACTORS = ['CLOSE', 'OPEN','TRADES', 'VOLUME', 'BUY_RATIO']
Z_WEIGHTS = [1.75, 1.75, 1.25, 1.5, 1.75]   # Weights to apply to z-factors
Z_DIMEN = ['CANDLE', 'MEAN', 'STD', 'ZSCORE', 'XSCORE']
MA_WINDOW = 8
MA_THRESH = 0.1


# Thresholds
X_THRESH = 1.75
# BTCUSDT (300,10800) Close Z-score of -3 good setting for bounce.
Z_BOUNCE_THRESH = -5.00

# App
HOST="45.79.176.125"
MONGO_PORT = 27017
DB_NAME="crypto"
DB_DUMP_PATH="~/Dropbox/mongodumps"
DROPBOXD_PATH="/opt/dropbox/dropboxd"

# Logger
DEBUGFILE = "logs/debug.log"
LOGFILE = "logs/info.log"
SIGNALFILE = "logs/signals.log"
SIGNAL = 100
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
