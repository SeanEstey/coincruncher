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
        lambda tckr, mkt: tckr['24hPriceChange'] > 15
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
        "stoploss": -25.0,
        "entry": {
            "conditions": [
                lambda c, ss: 10 < ss['indicators']['rsi'] < 40
            ],
        },
        "target": {
            "conditions": [
                lambda c, ss, doc: ss['indicators']['rsi'] > 70
            ]
        },
        "failure": {
            "conditions": [
                lambda c, ss, doc: ss['indicators']['rsi'] < 5
            ]
        }
    },
    #---------------------------------------------------------------------------
    {
        "name": "macd",
        "ema": (12, 26, 9),
        "stoploss": -5.0,
        "entry": {
            "conditions": [
                lambda c, ss: ss['indicators']['macd'] > 0,
                lambda c, ss: ss['macd']['history'][-1]['bars'] < 2,
                lambda c, ss: ss['macd']['history'][-1]['priceY'] > 0,
                lambda c, ss: ss['macd']['history'][-1]['priceX'] > 0
            ]
        },
        "target": {
            "conditions": [
                lambda c, ss, doc: (0 < ss['indicators']['macd'] < ss['macd']['desc']['max']),
                lambda c, ss, doc: ss['interim']['pricetrend'] < 0
            ]
        },
        "failure": {
            "conditions": [
                lambda c, ss, doc: ss['indicators']['macd'] < 0
            ]
        }
    }
]
