# app.bot.scanner
from pprint import pprint
import logging
import pandas as pd
import numpy as np
from datetime import timedelta as delta
from binance.client import Client
import app
from app import strtofreq
from app.common.timer import Timer
from app.common.utils import utc_datetime as now, to_relative_str
import app.bot
from app.bot import pct_diff
from docs.rules import STRATS as strats
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

    # Calc indicators
    top = indicators(df)

    quote_symbol = candle['pair']
    top = top.rename(columns={'close':'price', 'tradedMoney':'quoteVol'})
    #top = top.join(_df)
    top = top[[
        'price', 'price.std', 'close - open', 'high - low',
        'emaSlope', 'quoteVol', 'quoteAsset', 'buyRatio'
    ]]
    top = top.sort_values(['close - open', 'price.std'])

    lines = top.tail(n).to_string(formatters={
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

#------------------------------------------------------------------------------
def indicators(df):
    freq_str, freq = '1h', 3600
    start_str, periods = '48 hours ago utc', 48

    top = df.sort_values('close - open')#.tail(n)

    _df = pd.DataFrame(
        columns=[
            '∆(Close - Open)',
            '∆(High - Low)',
            'SD(∆P)',
            'Σ(∆P > 0)',
            'μ(∆P > 0)',
            'Σ(∆P < 0)',
            'μ(∆P < 0)',
            '+MACD/-MACD',
            '∆ema',
            'BuyVol',
        ],
        index=top.index
    ).astype('float64')

    for pair, row in df.iterrows():
        # API query/load candle data
        candles.update([pair], freq_str, start=start, force=True)
        app.bot.dfc = candles.merge_new(app.bot.dfc, [pair],
            span=now()-parse(start_str))
        dfc = app.bot.dfc.loc[pair].xs(freq, level=0).tail(periods)
        candle = candles.newest(pair, freq_str, df=app.bot.dfc)

        # Calc indicators
        pct_close_std = np.float64(dfc['close'].pct_change().describe()['std'] * 100)
        br_mean = dfc['buy_ratio'].describe()['mean'] * 100
        ema_slope = signals.ema_pct_change(candle, strats['ema']['span']).iloc[-1]

        # Price movement (total, velocity, momentum)
        pdelta = dfc['close'].tail(24).pct_change() * 100
        pos_pdelta = pdelta[pdelta > 0]
        neg_pdelta = pdelta[pdelta < 0]
        macd = signals.macd(dfc,
            strats['macd']['fast_span'],
            strats['macd']['slow_span']
        )
        pos_mom = macd[macd['macd_diff'] > 0]['macd_diff'].sum()
        neg_mom = abs(macd[macd['macd_diff'] < 0]['macd_diff'].sum())
        mom_ratio = pos_mom / neg_mom

        # MACD Histogram analysis.
        # Iterate through macd_diff, group histograms, calc avg length/depth

        _df.loc[pair] = [
            # done already,
            # done already,
            pct_close_std,
            p_up.sum(),
            p_up.mean(),
            p_down.sum(),
            p_down.mean(),
            mom_ratio,
            br_mean,
            ema_slope,
        ]
    return _df
