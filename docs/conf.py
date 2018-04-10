"""conf.py

Settings for formatting/subscribing to API data and trading bot.
"""

#### App #####################################################################

host = "45.79.176.125"
mongo_port = 27017
db_name = "coincruncher"

# DB backup (daily)
db_dump_path = "~/Dropbox/mongodumps"
dropboxd_path = "/opt/dropbox/dropboxd"

# Custom logging.logger levels for separate logfile data
SCAN = 98
TRADE = 99
SIGNAL = 100

debugfile = "logs/debug.log"
logfile = "logs/info.log"
signalfile = "logs/signals.log"
scannerfile = "logs/scanner.log"
tradefile = "logs/trade.log"

max_log_date_width = 14
max_log_name_width = 8
max_log_line_width = 125
log_newl_indent = 25

### Screen Client #############################################################

disp_refresh_delay = 30000
disp_scroll_sp = 5
disp_pad_height = 200

### Trade Indicators ##########################################################

zscore_thresh = (-3.0, 3.0)

ema9 = (9,)

# Ema spans for (fast,slow,signal)
macd_ema = (12,26,9)

### Trade Algorithms ###########################################################
#
# It's possible to have any number of simultaneously running strategies, with
# any config/callback combination, as long as the the dict keys below are
# unique.
################################################################################

# Max simultaneous open trades
max_positions = 10

stop_loss = 0.005

# Candle freqs for Binance WSS client to subscribe
trade_freq = [
    '1m',
    '5m',
    '1h',
    '1d'
]

# Trade pairs for Binance WSS client to subcribe
trade_pairs = [
    'BNBBTC',
    'BTCUSDT',
    'DGDBTC',
    'EOSBTC',
    'ENJBTC',
    'ONTBTC',
    'XVGBTC'
]

# Trading algo definitions.
trade_strategies = [
    {
        "name": "macd_5m_max",
        "ema": (12, 26, 9),
        # Entry on 5m MACD Histo > 0.
        "entry": {
            "filters": [lambda c, ss: c['freq'] == '5m'],
            "conditions": [lambda c, ss: ss['macd']['value'] > 0]
        },
        # Exit on 1m/5m MACD Histo < 0 or MAX.
        "exit": {
            "filters": [lambda c, ss, doc: c['freq'] in ['1m', '5m']],
            "conditions": [
                lambda c, ss, doc: ss['macd']['value'] < 0,
                lambda c, ss, doc: ss['macd']['value'] < ss['macd']['desc']['max'],
                lambda c, ss, doc: c['close'] < doc['orders'][0]['candle']['close'],
            ]
        }
    },
    {
        "name": "macd_5m_mean",
        "ema": (12, 26, 9),
        # Entry on 5m MACD Histo > 0.
        "entry": {
            "filters": [lambda c, ss: c['freq'] == '5m'],
            "conditions": [lambda c, ss: ss['macd']['value'] > 0]
        },
        # Exit on 1m/5m MACD Histo < 0 or MEAN.
        "exit": {
            "filters": [lambda c, ss, doc: c['freq'] in ['1m', '5m']],
            "conditions": [
                lambda c, ss, doc: ss['macd']['value'] < 0,
                lambda c, ss, doc: ss['macd']['value'] < ss['macd']['desc']['mean'],
                lambda c, ss, doc: c['close'] < doc['orders'][0]['candle']['close'],
            ]
        }
    }
]

### API Data ##################################################################

from app.common.utils import to_dt, to_int
# Candle format for both REST and WSS API
binance = {
    "trade_amt": 50.00,
    "pct_fee": 0.05,
    "kline_fields": [
        'open_time',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'close_time',
        # Total quote asset vol
        'quote_vol',
        'trades',
        # Taker buy base asset vol
        'buy_vol',
        # Taker buy quote asset vol
        'buy_quote_vol',
        'ignore'
    ]
}
# REST API
coinmarketcap = {
    'ticker_limit': 500,
    'currency': 'cad',
    "watch": [
        "LTC", "BCH", "XMR", "NEBL", "OCN", "BLZ", "ETC", "BNB", "BTC", "LINK",
        "DRGN", "ENJ", "EOS", "ETH", "GAS", "ICX", "JNT", "NANO", "NCASH", "NEO",
        "ODN", "OMG", "POLY", "REQ", "AGI", "VEN", "WTC", "ZIL", "ZCL", "XRP"
    ],
    "correlation": [
        "BLZ", "BTC", "BCH", "ETH", "EOS", "ETC", "ICX", "LTC", "NANO", "NEBL",
        "NEO", "OMG", "VEN", "XMR", "XRP", "WTC"
    ],
    "api": {
        "markets": [
            {
                "from":"last_updated",
                "to":"date",
                "type":to_dt},
            {
                "from":"total_market_cap_usd",
                "to":"mktcap_usd",
                "type":to_int},
            {
                "from":"total_24h_volume_usd",
                "to":"vol_24h_usd",
                "type":to_int},
            {
                "from":"bitcoin_percentage_of_market_cap",
                "to":"pct_mktcap_btc",
                "type":float
            },
            {
                "from":"active_assets",
                "to":"n_assets",
                "type":to_int
            },
            {
                "from":"active_currencies",
                "to":"n_currencies",
                "type":to_int
            },
            {
                "from":"active_markets",
                "to":"n_markets",
                "type":to_int
            }
        ],
        "tickers": [
            {
                "from":"id",
                "to":"id",
                "type":str
            },
            {
                "from":"symbol",
                "to":"symbol",
                "type":str
            },
            {
                "from":"name",
                "to":"name",
                "type":str
            },
            {
                "from":"last_updated",
                "to":"date",
                "type":to_dt
            },
            {
                "from":"rank",
                "to":"rank",
                "type":to_int
            },
            {
                "from":"market_cap_usd",
                "to":"mktcap_usd",
                "type":to_int
            },
            {
                "from":"24h_volume_usd",
                "to":"vol_24h_usd",
                "type":to_int
            },
            {
                "from":"available_supply",
                "to":"circulating_supply",
                "type":to_int
            },
            {
                "from":"total_supply",
                "to":"total_supply",
                "type":to_int
            },
            {
                "from":"max_supply",
                "to":"max_supply",
                "type":to_int
            },
            {
                "from":"price_usd",
                "to":"price_usd",
                "type":float
            },
            {
                "from":"percent_change_1h",
                "to":"pct_1h",
                "type":float
            },
            {
                "from":"percent_change_24h",
                "to":"pct_24h",
                "type":float
            },
            {
                "from":"percent_change_7d",
                "to":"pct_7d",
                "type":float
            }
        ]
    }
}
