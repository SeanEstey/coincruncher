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

Observations:
    BTCUSDT (300,10800) Close Z-score of -3 good setting for bounce.
"""

# Signals
Z_IDX_NAMES = ['PAIR', 'FREQ', 'CLOSE_TIME']
Z_DIMEN = ['CANDLE', 'MEAN', 'STD', 'ZSCORE', 'XSCORE']
Z_FACTORS = ['CLOSE', 'OPEN', 'TRADES', 'VOLUME', 'BUY_RATIO']
# Weights to apply to z-factors. Sum == Length of list
Z_WEIGHTS = [1.25,    0.0,    0.0,      1.5,      2.25]

# Trading Criteria for each Candle period
TRADING = {
    "5m": {
        # Number of time intervals for Moving Average
        "MA_Period": 10,
        # Magnitude of positive price trend
        "MA_Threshold": 0.1,
        # Predicts movement upward toward mean (within standard range)
        "Z_Score_Bounce_Thresh": -1.5,
        # Predict movement rising above standard range
        "Z_Score_Breakout_Thresh": 1.75,
        # Predict movement falling below standard range
        "Z_Score_Dump_Thresh": -5
    },
    "1h": {
        "MA_Period": 2,
        "MA_Threshold": 0.1
    }
}
