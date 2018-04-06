# app.bnc.scanner
from pprint import pprint
import logging
import pandas as pd
import numpy as np
from datetime import timedelta as delta
from binance.client import Client
import app
from app import strtofreq
from app.common.timer import Timer
from app.common.utils import utc_datetime as now
import app.bnc
from docs.rules import RULES as rules
from app import freqtostr
from . import candles, signals
log = logging.getLogger('scanner')

def scanlog(msg): log.log(98, msg)

#------------------------------------------------------------------------------
def update(n, idx_filter=None):
    columns = [
        'status', 'active', 'open', 'high', 'low', 'close',  'tradedMoney',
        'quoteAsset'
    ]
    client = Client("","")
    products = client.get_products()
    df = pd.DataFrame(products['data'],
        columns=columns,
        index = pd.Index([x['symbol'] for x in products['data']]) #, name='pair')
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

    # Filter top performers
    top = df.sort_values('close - open').tail(n)

    # Query 1h candles so we can calc STD, EMA, etc
    _df = pd.DataFrame(columns=['price.std','buyRatio','emaSlope'], index=top.index).astype('float64')

    for idx, row in top.iterrows():
        periods = 24
        freq_str = '1h'

        candles.update([idx], freq_str, start="24 hours ago utc")
        app.bnc.dfc = candles.merge_new(app.bnc.dfc, [idx], span=delta(hours=periods))
        hist = app.bnc.dfc.loc[idx].xs(strtofreq[freq_str], level=0).tail(periods)
        desc = hist.describe()
        pct_desc = hist.pct_change().describe()
        candle = candles.newest(idx, freq_str, df=app.bnc.dfc)

        _df.loc[idx] = [
            np.float64(pct_desc['close']['std']) * 100,
            desc['buy_ratio']['mean'] * 100,
            signals.ema_pct_change(candle, rules['ema']['span']).iloc[-1]
        ]

    quote_symbol = candle['pair']
    top = top.rename(columns={'close':'price', 'tradedMoney':'quoteVol'})
    top = top.join(_df)
    top = top[[
        'price', 'price.std', 'close - open', 'high - low',
        'emaSlope', 'quoteVol', 'quoteAsset', 'buyRatio'
    ]]
    top = top.sort_values(['close - open', 'price.std'])

    lines = top.to_string(formatters={
        "price": '{:>15.8g}'.format,
        "price.std": '{:>10.2f}%'.format,
        "close - open": '{:>+10.1f}%'.format,
        "high - low": '{:>+10.1f}%'.format,
        "emaSlope": '{:>+10.2f}'.format,
        "quoteVol": '{:>13,.0f}'.format,
        "quoteAsset": '{:>10}'.format,
        "buyRatio": '{:>10.1f}%'.format
    }).split("\n")

    scanlog('-' * 80)
    scanlog('Volatile/High Volume Trading Pairs in Past 24 Hours')
    [ scanlog(line) for line in lines]

    return top
