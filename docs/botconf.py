"""botconf.py

Settings for formatting/subscribing to API data and trading bot.
"""

# Max simultaneous open trades
max_positions = 10

# Trade pairs for Binance WSS client to subcribe
trade_pairs = [
    #'ADABTC',
    'AIONBTC',
    'BNBBTC',
    #'BTCUSDT',
    #'DGDBTC',
    #'DNTBTC',
    #'ELFBTC',
    #'ETHUSDT',
    #'EOSBTC',
    #'ENJBTC',
    'FUNBTC',
    #'ICXBTC',
    #'HSRBTC',
    'LRCBTC',
    #'OMGBTC',
    #'POWRBTC',
    #'ONTBTC',
    'OSTBTC',
    #'SALTBTC',
    #'STEEMBTC',
    'SUBBTC',
    #'XVGBTC',
    #'WABIBTC',
    #'WANBTC',
    #'WTCBTC',
    'ZILBTC'
]

### Trade Algorithms ###########################################################
# It's possible to have any number of simultaneously running strategies, with
# any config/callback combination. "name" is used as the primary key.
################################################################################
strategies = [
    #---------------------------------------------------------------------------
    {
        "name": "macd_30m_peak",
        "ema": (12, 26, 9),
        "stop_loss": {"freq": ["5m"], "pct": -0.75},
        "entry": {
            "filters": [lambda c, ss: c['freq'] in ['30m']],
            "conditions": [
                lambda c,ss: ss['macd']['value'] < 0,
                lambda c,ss: ss['macd']['value'] > ss['macd']['desc']['min'],
                lambda c,ss: ss['macd']['trend'] == 'UPWARD'
            ]
        },
        "exit": {
            "filters": [lambda c, ss, doc: c['freq'] in ['30m']],
            "conditions": [
                lambda c,ss,doc: ss['macd']['value'] > 0,
                lambda c,ss,doc: ss['macd']['value'] < ss['macd']['desc']['max'],
                lambda c,ss,doc: ss['macd']['trend'] == 'DOWNWARD'
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
                lambda c,ss: ss['macd']['value'] < 0,
                lambda c,ss: ss['macd']['value'] > ss['macd']['desc']['min'],
                lambda c,ss: ss['macd']['trend'] == 'UPWARD'
            ]
        },
        "exit": {
           "filters": [lambda c, ss, doc: c['freq'] in ['30m']],
           "conditions": [
               lambda c,ss,doc: ss['macd']['value'] > 0,
               lambda c,ss,doc: ss['macd']['value'] < ss['macd']['desc']['mean'],
               lambda c,ss,doc: ss['macd']['trend'] == 'DOWNWARD'
           ]
        }
    },
    #---------------------------------------------------------------------------
    {
        "name": "macd_30m_phase",
        "ema": (12, 26, 9),
        "stop_loss": {"freq": ['5m'], "pct": -0.75},
        "entry": {
            "filters": [lambda c, ss: c['freq'] in ['30m']],
            "conditions": [
                lambda c,ss: ss['macd']['phases']['data'][-1]['amp_mean'] > 0,
                lambda c,ss: ss['macd']['phases']['data'][-1]['bars'] < 5
            ]
        },
        "exit": {
            "filters": [lambda c, ss, doc: c['freq'] in ['30m']],
            "conditions": [
                lambda c,ss: ss['macd']['phases'][-1]['amp_mean'] > 0,
                lambda c,ss,doc: ss['macd']['value'] < ss['macd']['desc']['max'],
                lambda c,ss,doc: ss['macd']['trend'] == 'DOWNWARD'
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
                lambda c,ss: ss['macd']['value'] < 0,
                lambda c,ss: ss['macd']['value'] > ss['macd']['desc']['min'],
                lambda c,ss: ss['macd']['trend'] == 'UPWARD'
            ]
        },
        "exit": {
            "filters": [lambda c, ss, doc: c['freq'] in ['1h']],
            "conditions": [
                lambda c,ss,doc: ss['macd']['value'] > 0,
                lambda c,ss,doc: ss['macd']['value'] < ss['macd']['desc']['max'],
                lambda c,ss,doc: ss['macd']['trend'] == 'DOWNWARD'
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
                lambda c,ss: ss['macd']['value'] < 0,
                lambda c,ss: ss['macd']['value'] > ss['macd']['desc']['min'],
                lambda c,ss: ss['macd']['trend'] == 'UPWARD'
            ]
        },
        "exit": {
           "filters": [lambda c, ss, doc: c['freq'] in ['1h']],
           "conditions": [
               lambda c,ss,doc: ss['macd']['value'] > 0,
               lambda c,ss,doc: ss['macd']['value'] < ss['macd']['desc']['mean'],
               lambda c,ss,doc: ss['macd']['trend'] == 'DOWNWARD'
           ]
        }
    },
    #---------------------------------------------------------------------------
    {
        "name": "macd_1h_phase",
        "ema": (12, 26, 9),
        "stop_loss": {"freq": ['5m'], "pct": -0.75},
        "entry": {
            "filters": [lambda c, ss: c['freq'] in ['1h']],
            "conditions": [
                lambda c,ss: ss['macd']['value'] > 0,
                lambda c,ss: ss['macd']['trend'] == 'UPWARD'
            ]
        },
        "exit": {
            "filters": [lambda c, ss, doc: c['freq'] in ['1h']],
            "conditions": [
                lambda c,ss,doc: ss['macd']['value'] < ss['macd']['desc']['max'],
                lambda c,ss,doc: ss['macd']['trend'] == 'DOWNWARD'
            ]
        }
    }
]
