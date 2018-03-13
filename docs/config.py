# Trading
ZSCORE_THRESHOLD = 2

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
FREQ = 30
CURRENCY = "cad"

# Data
FREQ_TO_STR = {
    300:"5m",
    3600:"1h",
    86400:"1d"
}
PER_TO_STR = {
    3600:"60m",
    7200:"120m",
    10800: "180m",
    86400:"24h",
    172800:"48h",
    259200:"72h",
    604800:"7d",
    1209600:"14d",
    1814400:"21d"
}
