"""Trading strategies and config values.
Thesis:
    -During sideways trading, pairs exhibit patterns falling into a standard range
    -i.e Price will oscillate above/below a certain mean value with an average deviation
    -When price exceeds the average deviation too much, it is pulled back toward the mean
    -By analyzing the historical movements, we calculate both mean and standard deviation for
    a specific period
    -When analyzing a new candle, we can easily determine where its closing properties fall within the
    spectrum and use that history as a guide to predict its future movements
    -i.e If a closing candle price has a Z-Score of -2.0 (2 deviations < mean), we can expect it to reach
    bottom soon and move upward toward the mean. Similarly, deviations above the mean are statistically
    likely to be pulled back down toward the mean.
    -In a bull or bear market, the patterns are similar with the addition that the mean itself is now
    moving up and down. It needs to be recalculated in order to assess where new candle properties
    fall within the range.
"""

RULES = {
    "5m": {                                 # 5 minute candle
        "MOVING_AVG": {
            "PERIODS": 10,                  # Num periods
            "MARKET_THRESH": 0,             # Minimum
            "CANDLE_THRESH": 0.1            # Minimum
        },
        "Z-SCORE": {
            "TOP_RESIST":  3.0,             # Max num deviations from μ within {a,b}
            "BOT_RESIST": -3.0,              # Min num deviations from μ within {a,b}
            "BREAKOUT": 5.0,                # Num deviations > b in {a,b}
        },
        "X-SCORE": {                        # Units: number deviations from μ
            "WEIGHTS": [                    # Applied to Z-Scores
                1.25, 0, 0, 1.5, 2.25
            ],
            "DUMP": -5.0                    # Num deviations < a in {a,b}
        },
        "PAIRS": {                          # Custom settings for specific pairs
            "EOSBTC": None,                 # -2% ΔP often followed by +0.5-1.0% ΔP candle.
            "BTCUSDT": None                 # Close Z-score of -3 good setting for bounce.
        }
    },
    "1m": {
        "MOVING_AVG": {
            "PERIODS": 10,
            "MARKET_THRESH": None,
            "CANDLE_THRESH": 0.1
        },
        "Z-SCORE": {
            "TOP_RESIST": None,
            "BOT_RESIST": -2.0,
            "BREAKOUT": 5.0
        },
        "X-SCORE": {
            "WEIGHTS": [
                1.25, 0, 0, 1.5, 2.25
            ],
            "DUMP": 0
        },
        "PAIRS": {}
    },
    "1h": {
        "MOVING_AVG": {
            "PERIODS": 2,
            "MARKET_THRESH": None,
            "CANDLE_THRESH": None
        },
        "Z-SCORE": {
            "TOP_RESIST": None,
            "BOT_RESIST": None,
            "BREAKOUT": None
        },
        "X-SCORE": {
            "WEIGHTS": [
                1.25, 0, 0, 1.5, 2.25
            ],
            "DUMP": None
        },
        "PAIRS": {}
    }
}
