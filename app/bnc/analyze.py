# analyze.py
import logging
import pandas as pd
import numpy as np
from datetime import timedelta as delta
from binance.client import Client
import app
from app import strtofreq
from docs.data import BINANCE
from app.common.timer import Timer
from app.common.utils import utc_datetime as now
import app.bnc
from app import freqtostr
from . import candles, signals
log = logging.getLogger('analyze')

def analyze_log(msg): log.log(98, msg)

#------------------------------------------------------------------------------
def top_performers(n, idx_filter=None):
    columns = ['status', 'active', 'open', 'high', 'low', 'close', 'tradedMoney', 'volume']

    client = Client("","")
    products = client.get_products()

    df = pd.DataFrame(products['data'],
        columns=columns,
        index = pd.Index([x['symbol'] for x in products['data']], name='pair')
    )

    # Filter out inactive pairs
    df = df[df['active'] == True]
    del df['status']
    del df['active']
    df = df.astype('float64')

    # Custom filter str
    if idx_filter:
        df = df[df.index.str.contains(idx_filter)]

    # Compute metrics
    df['% c-o'] = (((df['close'] - df['open']) / df['open']) * 100).round(2)
    df['% h-l'] = ((df['high'] - df['low']) / df['low'] * 100).round(2)
    df = df.sort_values('% c-o')

    # Filter top performers
    top = df.tail(n)

    # Query 1h candles so we can calc STD, EMA, etc
    _df = pd.DataFrame(columns=['% c.std','bv.mean','ema.slope'], index=top.index).astype('float64')

    for idx, row in top.iterrows():
        periods = 24
        freq_str = '1h'

        print('idx: %s' % idx)
        candles.update([idx], freq_str, start="24 hours ago utc")
        app.bnc.dfc = candles.merge_new(app.bnc.dfc, [idx], span=delta(hours=periods))
        hist = app.bnc.dfc.loc[idx].xs(strtofreq[freq_str], level=0).tail(periods)
        desc = hist.describe()
        pct_desc = hist.pct_change().describe()
        candle = candles.newest(idx, freq_str, df=app.bnc.dfc)

        _df.loc[idx] = [
            np.float64(pct_desc['close']['std']) * 100,
            desc['buy_ratio']['mean'] * 100,
            signals.ema_pct_change(candle).iloc[-1]
        ]

    top = top.rename(columns={'close':'c', 'tradedMoney':'usd_vol', 'volume':'vol'})
    top = top.join(_df)
    top = top[['c', '% c.std', '% c-o', '% h-l', 'ema.slope', 'vol', 'bv.mean', 'usd_vol']]

    lines = top.to_string(formatters={
        "c": '{:>15.8f}'.format,
        "% c.std": '{:>10.2f}%'.format,
        "% c-o": '{:>+10.2f}%'.format,
        "% h-l": '{:>+10.2f}%'.format,
        "ema.slope": '{:>+12.2f}'.format,
        "vol": '{:>15.2f}'.format,
        "bv.mean": '{:>10.1f}%'.format,
        "usd_vol": '{:>14,.2f}'.format
    }).split("\n")
    [ analyze_log(line) for line in lines]

    return top
