"""
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
MAX_POSITIONS = 6
STOP_LOSS = 0.005
STRATS = {
    "macd": {
        'freq': ['5m'],
        'fast_span': 12,            # Price EMA line
        'slow_span': 26,            # Price EMA line
        'min_volume_zscore': 2,     # Adjust this higher in bearish markets for stronger bullish reversal confirmation
        'min_buy_ratio': 0.5,       # Ditto
        'min_momo_ratio': 1         # Momentum ratio. Sum(Histo > 0) / Sum(Histo < 0)
    },
    "ema": {
        "span": 20
    },
    "z-score": {
        "periods": 36,              # Avg macd histogram full cycle on 5m is 3 hrs (36 periods)
        "buy_thresh": -3.0,
        "sell_thresh": 0.0
    }
}
