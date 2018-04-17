"""botconf.py
Settings for formatting/subscribing to API data and trading bot.
"""
tradepairs = {
    "filters": [
        # Aggregate parent symbol (quote asset) market price
        lambda tckr, mkt: mkt['24h.Δprice'] > 0,
        # Individual pair price.
        lambda tckr, mkt: tckr['24hPriceChange'] > 7.5
    ],
    "conditions": [
        # Positive macd oscilator phase
        lambda ss: ss['macd']['history'][-1]['ampMean'] > 0,
        # Vertical price movement within oscilator phase.
        lambda ss: ss['macd']['history'][-1]['priceY'] > 0,
        # Sustained vertical price over X-axis (current)
        lambda ss: ss['macd']['history'][-1]['priceX'] > 0
   ]
}

macd_scan = [
    #{'freqstr':'5m', 'startstr':'36 hours ago utc', 'periods':350}
    {'freqstr':'30m', 'startstr':'72 hours ago utc', 'periods':100}
    #{'freqstr':'1h', 'startstr':'72 hours ago utc', 'periods':72},
]

### Trade Algorithms ###########################################################
# It's possible to have any number of simultaneously running strategies, with
# any config/callback combination. "name" is used as the primary key.
################################################################################
strategies = [
    #---------------------------------------------------------------------------
    {
        "name": "macd5m",
        "ema": (12, 26, 9),
        "stop_loss": {"freq": ['5m'], "pct": -0.75},
        "entry": {
            "filters": [],
            "conditions": [
                lambda c,ss: ss['macd']['values'][-1] > 0,
                lambda c,ss: ss['macd']['trend'] > 0,
                lambda c,ss: ss['macd']['history'][-1]['bars'] < 5,
                lambda c,ss: ss['macd']['history'][-1]['priceX'] > 0,
                lambda c,ss: ss['rsi'] <= 0.3
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
    },
    #---------------------------------------------------------------------------
    {
        "name": "macd30m",
        "ema": (12, 26, 9),
        "stop_loss": {"freq": ['5m'], "pct": -0.75},
        "entry": {
            "filters": [],
            "conditions": [
                lambda c,ss: ss['macd']['values'][-1] > 0,
                lambda c,ss: ss['macd']['trend'] > 0,
                lambda c,ss: ss['macd']['history'][-1]['bars'] < 5,
                lambda c,ss: ss['macd']['history'][-1]['priceX'] > 0,
                lambda c,ss: ss['rsi'] <= 0.3

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
    },
    #---------------------------------------------------------------------------
    {
        "name": "macd1h",
        "ema": (12, 26, 9),
        "stop_loss": {"freq": ['5m'], "pct": -0.75},
        "entry": {
            "filters": [],
            "conditions": [
                lambda c,ss: ss['macd']['values'][-1] > 0,
                lambda c,ss: ss['macd']['trend'] > 0,
                lambda c,ss: ss['macd']['history'][-1]['bars'] < 5,
                lambda c,ss: ss['macd']['history'][-1]['priceX'] > 0,
                lambda c,ss: ss['rsi'] <= 0.3
            ]
        },
        "exit": {
            "filters": [lambda c, ss, doc: c['freq'] in ['1h']],
            "conditions": [
                lambda c,ss,doc: ss['macd']['values'][-1] < ss['macd']['desc']['max'],
                lambda c,ss,doc: ss['macd']['trend'] < 0
            ]
        }
    }
]
"""
#---------------------------------------------------------------------------
{
    "name": "macd_30m_peak",
    "ema": (12, 26, 9),
    "stop_loss": {"freq": ["5m"], "pct": -0.75},
    "entry": {
        "filters": [lambda c, ss: c['freq'] in ['30m']],
        "conditions": [
            lambda c,ss: ss['macd']['values'][-1] < 0,
            lambda c,ss: ss['macd']['values'][-1] > ss['macd']['desc']['min'],
            lambda c,ss: ss['macd']['trend'] > 0
        ]
    },
    "exit": {
        "filters": [lambda c, ss, doc: c['freq'] in ['30m']],
        "conditions": [
           lambda c,ss,doc: ss['macd']['values'][-1] > 0,
           lambda c,ss,doc: ss['macd']['values'][-1] < ss['macd']['desc']['max'],
           lambda c,ss,doc: ss['macd']['trend'] < 0
        ]
    }
},
#---------------------------------------------------------------------------
{
    "name": "macd_30m_mean",
    "ema": (12, 26, 9),
    "stop_loss": {"freq": ['5m'], "pct": -0.75},
    "entry": {
        "filters": [lambda c, ss: c['freq'] in ['30m']],
        "conditions": [
            lambda c,ss: ss['macd']['values'][-1] < 0,
            lambda c,ss: ss['macd']['values'][-1] > ss['macd']['desc']['min'],
            lambda c,ss: ss['macd']['trend'] > 0
        ]
    },
    "exit": {
       "filters": [lambda c, ss, doc: c['freq'] in ['30m']],
       "conditions": [
           lambda c,ss,doc: ss['macd']['values'][-1] > 0,
           lambda c,ss,doc: ss['macd']['values'][-1] < ss['macd']['desc']['mean'],
           lambda c,ss,doc: ss['macd']['trend'] < 0
       ]
    }
},
#---------------------------------------------------------------------------
{
    "name": "macd_1h_peak",
    "ema": (12, 26, 9),
    "stop_loss": {"freq": ["5m", "1h"], "pct": -0.75},
    "entry": {
        "filters": [lambda c, ss: c['freq'] in ['1h']],
        "conditions": [
            lambda c,ss: ss['macd']['values'][-1] < 0,
            lambda c,ss: ss['macd']['values'][-1] > ss['macd']['desc']['min'],
            lambda c,ss: ss['macd']['trend'] > 0
        ]
    },
    "exit": {
        "filters": [lambda c, ss, doc: c['freq'] in ['1h']],
        "conditions": [
            lambda c,ss,doc: ss['macd']['values'][-1] > 0,
            lambda c,ss,doc: ss['macd']['values'][-1] < ss['macd']['desc']['max'],
            lambda c,ss,doc: ss['macd']['trend'] < 0
        ]
    }
},
#---------------------------------------------------------------------------
{
    "name": "macd_1h_mean",
    "ema": (12, 26, 9),
    "stop_loss": {"freq": ['5m'], "pct": -0.75},
    "entry": {
        "filters": [lambda c, ss: c['freq'] in ['1h']],
        "conditions": [
            lambda c,ss: ss['macd']['values'][-1] < 0,
            lambda c,ss: ss['macd']['values'][-1] > ss['macd']['desc']['min'],
            lambda c,ss: ss['macd']['trend'] > 0
        ]
    },
    "exit": {
       "filters": [lambda c, ss, doc: c['freq'] in ['1h']],
       "conditions": [
           lambda c,ss,doc: ss['macd']['values'][-1] > 0,
           lambda c,ss,doc: ss['macd']['values'][-1] < ss['macd']['desc']['mean'],
           lambda c,ss,doc: ss['macd']['trend'] < 0
       ]
    }
}
"""
