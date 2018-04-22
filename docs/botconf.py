# botconf
# Configuration and algorithm definitions for trading bot.

CACHE_SIZE = 500
DEF_KLINE_HIST_LEN = "72 hours ago utc"
TRADE_AMT_MAX = 50.00
TRADEFREQS = ['5m', '30m', '1h']

# Algorithm that decides which trading pairs are enabled
# @tckr: binance ticker dataframe
# @mkt: binance aggregate market dataframe for parent symbol (quote asset)
# @ss: trade.snapshot() result w/ indicators
TRADE_PAIR_ALGO = {
    "filters": [
        lambda tckr, mkt: mkt['24hPriceChange'] > 0,
        lambda tckr, mkt: tckr['24hPriceChange'] > 10.5
    ],
    "conditions": [
        lambda ss: ss['macd']['history'][-1]['ampMean'] > 0,
        lambda ss: ss['macd']['history'][-1]['priceY'] > 0,
        lambda ss: ss['macd']['history'][-1]['priceX'] > 0
   ]
}
# Trading algorithm definitions. Any number can be running simultaneously as
# long as they have unique names.
# @c: candle dict, @ss: snapshot dict, @doc: mongo trade dict
TRADE_ALGOS = [
    {
        "name": "rsi",
        "ema": (12, 26, 9),
        "stoploss": -0.75,
        "entry": {
            "conditions": [
                lambda ss: 10 < ss['rsi'] < 40
            ],
        },
        "target": {
            "conditions": [
                lambda ss: ss['rsi'] > 70
            ]
        },
        "failure": {
            "conditions": [
                lambda ss: ss['rsi'] < 5
            ]
        }
    },
    #---------------------------------------------------------------------------
    {
        "name": "macd",
        "ema": (12, 26, 9),
        "stoploss": -0.75,
        "entry": {
            "conditions": [
                lambda ss: ss['macd']['value'] > 0,
                lambda ss: ss['macd']['bars'] < 2,
                lambda ss: ss['macd']['priceY'] > 0,
                lambda ss: ss['macd']['priceX'] > 0
            ]
        },
        "target": {
            "conditions": [
                lambda ss: (0 < ss['macd']['value'] < ss['macd']['ampMax']),
                lambda ss: ss['macd']['ampSlope'] < 0
            ]
        },
        "failure": {
            "conditions": [
                lambda ss: ss['macd']['value'] < 0 or ss['macd']['ampSlope'] < 0
            ]
        }
    }
]
