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
    "1m": {
        "EMA": {
            "SPAN": 20              # Num candle periods
        },
        "Z-SCORE": {
            "SMOOTH_SPAN": 5,       # Periods to use to smoothen signal
            "BUY_THRESH": -3.0,     # Buy threshold (deviations from μ)
            "SELL_THRESH": 0.0
        }
    }
}
