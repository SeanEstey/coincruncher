# botconf
# Configuration and algorithm definitions for trading bot.

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
        lambda tckr, mkt: tckr['24hPriceChange'] > 10.0
    ],
    "conditions": [
        lambda ss: ss['macd']['history'][-1]['ampMean'] > 0,
        lambda ss: ss['macd']['history'][-1]['priceY'] > 0,
        lambda ss: ss['macd']['history'][-1]['priceX'] > 0
   ]
}

# Can run any number of algorithms simultaneously as long as they have unique names.
# @c: candle dict
# @ss: trade.snapshot() result w/ indicators
# @doc: mongodb trade record dict
TRADE_ALGOS = [
    {
        "name": "macd5m",
        "ema": (12, 26, 9),
        "stoploss": -0.75,
        "entry": {
            "filters": [],
            "conditions": [
                lambda c,ss: ss['macd']['values'][-1] > 0,
                lambda c,ss: ss['macd']['trend'] > 0
                lambda c,ss: ss['macd']['history'][-1]['bars'] < 5,
                lambda c,ss: ss['macd']['history'][-1]['priceX'] > 0,
                lambda c,ss: ss['rsi'] <= 75
            ]
        },
        "exit": {
            "filters": [],
            "conditions": [
                lambda c,ss,doc: ss['macd']['values'][-1] > 0,
                lambda c,ss,doc: ss['macd']['values'][-1] < ss['macd']['desc']['max'],
                lambda c,ss,doc: ss['macd']['trend'] < 0
            ]
        }
    }, # END
    {
        "name": "macd30m",
        "ema": (12, 26, 9),
        "stoploss": -0.75,
        "entry": {
            "filters": [],
            "conditions": [
                lambda c,ss: ss['macd']['values'][-1] > 0,
                lambda c,ss: ss['macd']['trend'] > 0,
                lambda c,ss: ss['macd']['history'][-1]['bars'] < 5,
                lambda c,ss: ss['macd']['history'][-1]['priceX'] > 0,
                lambda c,ss: ss['rsi'] <= 50
            ]
        },
        "exit": {
            "filters": [],
            "conditions": [
                lambda c,ss,doc: ss['macd']['values'][-1] > 0,
                lambda c,ss,doc: ss['macd']['values'][-1] < ss['macd']['desc']['max'],
                lambda c,ss,doc: ss['macd']['trend'] < 0
            ]
        }
    }, # END
    {
        "name": "macd1h",
        "ema": (12, 26, 9),
        "stoploss": -0.75,
        "entry": {
            "filters": [],
            "conditions": [
                lambda c,ss: ss['macd']['values'][-1] > 0,
                lambda c,ss: ss['macd']['trend'] > 0,
                lambda c,ss: ss['macd']['history'][-1]['bars'] < 5,
                lambda c,ss: ss['macd']['history'][-1]['priceX'] > 0,
                lambda c,ss: ss['rsi'] <= 50
            ]
        },
        "exit": {
            "filters": [],
            "conditions": [
                lambda c,ss,doc: ss['macd']['values'][-1] < ss['macd']['desc']['max'],
                lambda c,ss,doc: ss['macd']['trend'] < 0
            ]
        }
    }   # END
]
