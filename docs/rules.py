"""
"""
TRADING_PAIRS = [
    'BTCUSDT',
    'EOSBTC',
    'OAXBTC',
    'ONTBTC',
    'POEBTC',
    'TRXBTC',
    'ZRXBTC',
]

MAX_POSITIONS = 6
RULES = {
    "macd": {
        'freq': ['5m'],
        "short_period": 12,
        "long_period": 26
    },
    "ema": {
        "span": 20              # Num candle periods
    },
    "z-score": {
        "periods": 20,       # Periods to use to smoothen signal
        "buy_thresh": -3.0,     # Buy threshold (deviations from μ)
        "sell_thresh": 0.0
    }
}
