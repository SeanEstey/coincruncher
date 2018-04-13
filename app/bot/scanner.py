# app.bot.scanner
from pprint import pprint
import logging
import math
import pandas as pd
import numpy as np
from datetime import timedelta as delta
from dateparser import parse
from binance.client import Client
import app
from app.common.timer import Timer
from app.common.utils import utc_datetime as now, to_relative_str, datestr_to_dt
import app.bot
from app.bot import pct_diff
from app.common.timeutils import strtofreq
from . import candles, macd, signals
log = logging.getLogger('scanner')

def scanlog(msg): log.log(98, msg)

#------------------------------------------------------------------------------
def scan(freqstr, periods, n_results, idx_filter=None, quiet=True):
    columns = ['status', 'active', 'open', 'high', 'low', 'close', 'tradedMoney']
    np_columns = ['open', 'high', 'low', 'close', 'tradedMoney']
    del_columns = ['status', 'active', 'high', 'low', 'open']

    # Query data
    client = Client("","")
    products = client.get_products()
    # To DataFrame
    df = pd.DataFrame(client.get_products()['data'],
        columns=columns,
        index = pd.Index([x['symbol'] for x in products['data']]))
    # Str->numpy float
    df[np_columns] = df[np_columns].astype('float64')

    # Filters
    df = df[df['active'] == True]
    if idx_filter:
        df = df[df.index.str.contains(idx_filter)]

    # Indicators
    df['close - open'] = (((df['close'] - df['open']) / df['open']) * 100).round(2)
    df['high - low'] = ((df['high'] - df['low']) / df['low'] * 100).round(2)
    df = df.sort_values('close - open').tail(n_results)
    df = df.join(indicators(df.index, freqstr, periods, quiet=quiet))

    # Filter indicators
    df = df[df['quoteVol'] >= 1000]

    # Format and print to scanlog
    for col in del_columns:
        del df[col]
    df = df.rename(columns={
        'close':'P',
        'close - open': '∆(C-O)',
        'high - low': '∆(H-L)',
        'tradedMoney':'quoteVol'
    })
    df = df[['P', '∆(C-O)', '∆(H-L)', 'Σ(MACD+)', 'μ(MACD+)', 'Σ(MACD-)',
        'μ(MACD-)', 'MACD(+/-)', 'quoteVol']]
    df = df.sort_values('μ(MACD+)')
    df = df[df['μ(MACD+)'] != np.nan] #.dropna()
    lines = df.to_string(formatters={
        'P': '{:.8g}'.format,
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
    [ scanlog(line) for line in lines]

    return df

#------------------------------------------------------------------------------
def indicators(idx, freqstr, periods, quiet=True):
    strunit=None
    if freqstr[-1] == 'm':
        strunit = 'minutes'
    elif freqstr[-1] == 'h':
        strunit = 'hours'
    elif freqstr[-1] == 'd':
        strunit = 'days'

    n = int(freqstr[:-1])
    startstr = "{} {} ago utc".format(n * periods + 25, strunit)
    pprint("startstr={}".format(startstr))
    freq = strtofreq(freqstr)

    df = pd.DataFrame(
        columns=[
            'Σ(MACD+)',
            'μ(MACD+)',
            'Σ(MACD-)',
            'μ(MACD-)',
            'MACD(+/-)'
        ],
        index=idx
    ).astype('float64').round(3)

    for pair, row in df.iterrows():
        # Query/load candle data
        candles.update([pair], freqstr, start=startstr, force=True)
        dfp = candles.merge_new(pd.DataFrame(), [pair], span=delta(days=7))
        dfp = dfp.loc[pair,freq]

        # Run MACD histogram analysis
        histos = macd.agg_describe(dfp, pair, freqstr, periods)

        if quiet != True:
            [ scanlog(line) for line in histos['summary'].split('\n') ]

        ppdiff = histos['stats']['POSITIVE']['price_diff']
        npdiff = histos['stats']['NEGATIVE']['price_diff']
        df.loc[pair] = [
            ppdiff['sum'],
            ppdiff['mean'],
            npdiff['sum'],
            npdiff['mean'],
            ppdiff['sum'] / npdiff['sum']
        ]

    scanlog('')
    scanlog('-' * 100)
    scanlog("MACD Analysis for {} Freq in Last {} Periods".format(freqstr,periods))
    scanlog("")

    return df.round(2)
