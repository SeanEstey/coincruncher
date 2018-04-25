# botconf
# Configuration and algorithm definitions for trading bot.


def pct(a,b): return ((b-a)/a)*100

##### General ##################################################################

DEF_KLINE_HIST_LEN = "72 hours ago utc"

##### Trading Conf #############################################################

TRD_AMT_MAX = 50.00
TRD_FREQS = ['5m', '30m', '1h', '1d']

# Trend definitions for algorithmic filtering of trading pairs.
# @sma: 1d moving avg dataframe
TRD_PAIRS = {
    "longterm": None,
    "shorterm": None,
    "midterm": {
        "name": "sma",
        "freqstr": "1d",
        "span": 5,
        "filters": [lambda tckr: tckr[tckr['24hPriceChange'] > 5].index.tolist()],
        "conditions": [lambda sma: len(sma) > 0 and sma.iloc[-1] > 2]
    }
}

# Trading algorithm definitions. No limits to number running concurrently in
# simulation mode.
# @ss: snapshot dict, @t: trade record dict
TRD_ALGOS = [
    {
        "name": "rsi",
        "ema": (12, 26, 9),
        "stoploss": -2.5,
        "entry": {
            "conditions": [
                lambda ss: 10 < ss['rsi'] < 40
            ],
        },
        "target": {
            "conditions": [
                lambda ss,st: ss['rsi'] > 70,
                lambda ss,st: pct(st['minPrice'], st['lastPrice']) > 0.75
           ]
        },
        "failure": {
            "conditions": [
                lambda ss,st: ss['rsi'] < 5
            ]
        }
    },
    {
        "name": "macd",
        "ema": (12, 26, 9),
        "stoploss": -0.75,
        "entry": {
            "conditions": [
                lambda ss: ss['macd']['value'] > 0,
                lambda ss: ss['macd']['bars'] < 3,
                lambda ss: ss['macd']['priceY'] > 0,
                lambda ss: ss['macd']['priceX'] > 0
            ]
        },
        "target": {
            "conditions": [
                lambda ss,st: ss['macd']['ampMax'] > ss['macd']['value'] > 0,
                lambda ss,st: pct(st['minPrice'], st['lastPrice']) > 0.75
            ]
        },
        "failure": {
            "conditions": [
                lambda ss,st: ss['macd']['value'] < 0 or ss['macd']['ampSlope'] < 0
            ]
        }
    }
]
