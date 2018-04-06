"""
"""
CANDLE_FREQ = [
    #'1m',
    '5m',
    '1h',
    '1d'
]
TRADING_PAIRS = [
    'BNBBTC',
    'BTCUSDT',
    'DGDBTC',
    'EOSBTC',
    'OAXBTC',
    'ONTBTC',
    'POEBTC',
    #'TRXBTC',
    #'ZRXBTC',
]
MAX_POSITIONS = 6
RULES = {
    "macd": {
        'freq': ['5m'],
        "short_period": 12,
        "long_period": 26
    },
    "ema": {
        "span": 20
    },
    "z-score": {
        "periods": 36,          # Avg macd histogram full cycle on 5m is 3 hrs (36 periods)
        "buy_thresh": -3.0,
        "sell_thresh": 0.0
    }
}
