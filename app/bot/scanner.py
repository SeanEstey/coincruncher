# app.bot.scanner
from pprint import pprint
import logging
import pandas as pd
import numpy as np
from datetime import timedelta as delta
from dateparser import parse
from binance.client import Client
import app
from app import strtofreq
from app.common.timer import Timer
from app.common.utils import utc_datetime as now, to_relative_str
import app.bot
from app.bot import pct_diff
from app import freqtostr
from . import candles, macd, signals
log = logging.getLogger('scanner')

def scanlog(msg): log.log(98, msg)

#------------------------------------------------------------------------------
def update(n, idx_filter=None):
    columns = [
        'status', 'active', 'open', 'high', 'low', 'close',  'tradedMoney'
    ]
    client = Client("","")
    products = client.get_products()
    df = pd.DataFrame(products['data'],
        columns=columns,
        index = pd.Index([x['symbol'] for x in products['data']])
    )

    # Filter out inactive pairs
    df = df[df['active'] == True]
    del df['status']
    del df['active']
    df[['open','high','low','close','tradedMoney']] = df[['open','high','low','close','tradedMoney']].astype('float64')

    # Custom filter str
    if idx_filter:
        df = df[df.index.str.contains(idx_filter)]

    # Compute metrics
    df['close - open'] = (((df['close'] - df['open']) / df['open']) * 100).round(2)
    df['high - low'] = ((df['high'] - df['low']) / df['low'] * 100).round(2)

    # Calc indicators
    dfi = indicators(df, n)
    df = df.rename(columns={
        'close':'P',
        'close - open': '∆(C-O)',
        'high - low': '∆(H-L)',
        'tradedMoney':'quoteVol'
    })
    df = df.join(dfi)
    df = df.dropna()
    del df['high']
    del df['low']
    del df['open']
    df = df[[
        'P',
        '∆(C-O)',
        '∆(H-L)',
        #'SD(∆P)',
        'Σ(MACD+)',
        'μ(MACD+)',
        'Σ(MACD-)',
        'μ(MACD-)',
        'MACD(+/-)',
        'quoteVol'
    ]]
    df = df.sort_values('μ(MACD+)')

    lines = df.tail(n).to_string(formatters={
        'P': '{:.8g}'.format,
        #'SD(∆P)': ' {:.2f}%'.format,
        '∆(C-O)': ' {:+.1f}%'.format,
        '∆(H-L)': ' {:+.1f}%'.format,
        'Σ(MACD+)': ' {:.2f}%'.format,
        'μ(MACD+)': ' {:.2f}%'.format,
        'Σ(MACD-)': ' {:.2f}%'.format,
        'μ(MACD-)': ' {:.2f}%'.format,
        'MACD(+/-)': '{:.2f}'.format,
        "quoteVol": '{:.0f}'.format
        #"buyRatio": '{:>10.1f}%'.format
    }).split("\n")

    scanlog('')
    scanlog('-' * 100)
    [ scanlog(line) for line in lines]

    return df

#------------------------------------------------------------------------------
def indicators(df, n):
    freqstr, freq = '1h', 3600
    startstr, periods = '48 hours ago utc', 48
    df_top = df.sort_values('close - open').tail(n)
    _df = pd.DataFrame(
        columns=[
            #'SD(∆P)',
            'Σ(MACD+)',
            'μ(MACD+)',
            'Σ(MACD-)',
            'μ(MACD-)',
            'MACD(+/-)'
        ],
        index=df_top.index
    ).astype('float64').round(3)

    for pair, row in df_top.iterrows():
        # Upade/load candle data
        candles.update([pair], freqstr, start=startstr, force=True)
        dfp = candles.merge_new(pd.DataFrame(), [pair], span=delta(days=7))
        dfp = dfp.loc[pair,freq]

        # Calc macd indicators
        scan = macd.agg_describe(dfp, pair, freqstr, 48, pdfreqstr='1H')
        ppdiff = scan['stats']['POSITIVE']['price_diff']
        npdiff = scan['stats']['NEGATIVE']['price_diff']
        _df.loc[pair] = [
            #sd,
            ppdiff['sum'],
            ppdiff['mean'],
            npdiff['sum'],
            npdiff['mean'],
            ppdiff['sum'] / npdiff['sum']
        ]
    return _df.round(2)
