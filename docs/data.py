from app.utils import to_dt, to_int

BINANCE = {
    "TRADE_AMT": 50.00,
    "PCT_FEE": 0.05,
    "PAIRS": [
        #"BLZBTC",
        "BNBUSDT",
        "QTUMUSDT",
        "BTCUSDT",
        "BCCUSDT",
        "ENGBTC",
        "EOSBTC",
        "ETCBTC",
        "ETHBTC",
        "ETHUSDT",
        "ICXBTC",
        "LINKBTC",
        "LTCUSDT",
        "NANOBTC",
        "NCASHBTC",
        "NEBLBTC",
        "NEOBTC",
        "NEOUSDT",
        "OMGBTC",
        "STORMBTC",
        #"VENBTC",
        #"VIABTC",
        "WANBTC",
        "WTCBTC",
        "XMRBTC",
        #"XRPBTC",
        "XVGBTC",
        #"ZECBTC"
    ],
    "KLINE_FIELDS": [
        'open_time',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'close_time',
        'quote_vol',        # Total quote asset vol
        'trades',
        'buy_vol',          # Taker buy base asset vol
        'buy_quote_vol',    # Taker buy quote asset vol
        'ignore'
    ]
}

COINMARKETCAP = {
    "WATCH": [
        "LTC", "BCH", "XMR", "NEBL", "OCN", "BLZ", "ETC", "BNB", "BTC", "LINK", "DRGN",
        "ENJ", "EOS", "ETH", "GAS", "ICX", "JNT", "NANO", "NCASH", "NEO", "ODN", "OMG",
        "POLY", "REQ", "AGI", "VEN", "WTC", "ZIL", "ZCL", "XRP"
    ],
    "CORR": [
        "BLZ", "BTC", "BCH", "ETH", "EOS", "ETC", "ICX", "LTC", "NANO", "NEBL", "NEO",
        "OMG", "VEN", "XMR", "XRP", "WTC"
    ],
    "PORTFOLIO": {
        "BNB": 11.17,
        "BTC": 6.90920,
        "LINK": 9500,
        "COV": 640.0,
        "DBC": 1250.57,
        "DRGN": 1319.65,
        "ENJ": 19948.00,
        "EOS": 247.29,
        "ETH": 7.08,
        "GAS": 0.8556,
        "ICX": 1260.03,
        "JNT": 749.25,
        "NANO": 3319.68,
        "NEO": 100.00,
        "ODN": 8637.0,
        "OMG": 77.2,
        "POLY": 3906.21,
        "REQ": 2529.44,
        "SAFEX": 17707.00,
        "AGI": 4735.75,
        "VEN": 481.00,
        "WTC": 100.00,
        "ZIL": 34721.56,
        "ZCL": 137
    },
    "API": {
        "MARKETS": [
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
        "TICKERS": [
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
