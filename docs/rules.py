"""docs.rules
Bot config for scanner and trading.
"""

CANDLE_FREQ = [
    '5m',
    '1h',
    '1d'
]
TRADING_PAIRS = [
    #'BNBBTC',
    'BTCUSDT',
    'DGDBTC',
    'EOSBTC',
    'ENJBTC',
    'ONTBTC',
    'XVGBTC'
]

#-------------------------------------------------------------------------------

MAX_POSITIONS = 6
STOP_LOSS = 0.005
STRATS = {
    "macd": {
        'freq': ['5m'],
        'ema': (12, 26, 9),      # ema spans: (fast, slow, signal)
        'min_volume_zscore': 2,
        'min_buy_ratio': 0.5,
        'min_momo_ratio': 1
    },
    "ema": {
        "span": 20
    },
    "z-score": {
        # Avg macd histogram full cycle on 5m is 3 hrs (36 periods)
        "periods": 36,
        "buy_thresh": -3.0,
        "sell_thresh": 0.0
    }
}
