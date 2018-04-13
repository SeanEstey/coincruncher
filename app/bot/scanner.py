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
def scan(freqstr, periods, n_results, min_vol, idx_filter=None, quiet=True):
    query_cols = ['status', 'active', 'open', 'high', 'low', 'close', 'tradedMoney']
    np_cols = ['open', 'high', 'low', 'close', 'tradedMoney']
    del_cols = ['status', 'active', 'high', 'low', 'open']

    # Query data
    client = Client("","")
    products = client.get_products()
    # To DataFrame
    df = pd.DataFrame(client.get_products()['data'],
        columns=query_cols,
        index=pd.Index([x['symbol'] for x in products['data']]))
    # Str->numpy float
    df[np_cols] = df[np_cols].astype('float64')

    # Filters
    df = df[df['active'] == True]
    if idx_filter:
        df = df[df.index.str.contains(idx_filter)]

    # Indicators
    # FIXME: Calculate C-O and H-L for given timespan only. These numbers are
    # for 24 hour period.
    df['∆(C-O)'] = (((df['close'] - df['open']) / df['open']) * 100).round(2)
    df['∆(H-L)'] = ((df['high'] - df['low']) / df['low'] * 100).round(2)
    df = df.sort_values('∆(C-O)').tail(n_results)
    results = indicators(df.index, freqstr, periods, quiet=quiet)

    # Format/filter columns some more
    for col in del_cols:
        del df[col]
    df = df.rename(columns={'close':'Price', 'tradedMoney':'quoteVol'})
    df = df[df['quoteVol'] >= min_vol]

    scanlog('')
    scanlog('-' * 92)
    scanlog("Scanner Analysis")
    scanlog("Freq: '{}', Periods: {}".format(freqstr, periods))
    scanlog("Symbol Filter: '{}'".format(idx_filter))
    scanlog("Volume Filter: > {}".format(min_vol))

    k1, k2, k3 = 'candle', 'macd+', 'macd-'
    df1 = pd.concat([df[['Price', '∆(C-O)', '∆(H-L)', 'quoteVol']]],
        axis=1, keys=[k1])
    df2 = pd.concat([results[0]], axis=1, keys=[k2])
    df3 = pd.concat([results[1]], axis=1, keys=[k3])
    df = df1.join(df2)
    df = df.join(df3)

    lines = df.to_string(formatters={
        (k1,'Price'):      '{:.8g}'.format,
        (k1, 'quoteVol'):  '{:.0f}'.format,
        (k1, '∆(C-O)'):    ' {:+.1f}%'.format,
        (k1, '∆(H-L)'):    ' {:+.1f}%'.format,
        (k2, 'N'):      '{:}'.format,
        (k2, 'Amp'):    '{:.2f}'.format,
        (k2, '∆P'):     ' {:.2f}%'.format,
        (k2, 'μ∆P'):    ' {:.2f}%'.format,
        (k2, 'μT'):     '{:}'.format,
        (k3, 'N'):      '{:}'.format,
        (k3, 'Amp'):    '{:.2f}'.format,
        (k3, '∆P'):     ' {:.2f}%'.format,
        (k3, 'μ∆P'):    ' {:.2f}%'.format,
        (k3, 'μT'):     '{:}'.format
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
    results=[]

    for sign in ['POSITIVE', 'NEGATIVE']:
        df = pd.DataFrame(
            columns=['N', 'Amp', '∆P', 'μ∆P', 'μT'],
            index=idx
        ).astype('float64')

        for pair, row in df.iterrows():
            # Query/load candle data
            candles.update([pair], freqstr, start=startstr, force=True)
            dfp = candles.merge_new(pd.DataFrame(), [pair], span=delta(days=7))
            dfp = dfp.loc[pair,freq]

            # Run MACD histogram analysis
            histos = macd.agg_describe(dfp, pair, freqstr, periods)
            stats = histos['stats'][sign]
            df.loc[pair] = [
                stats['n_phases'],
                stats['mean_amplitude'],
                stats['price_diff']['sum'],
                stats['price_diff']['mean'],
                stats['duration']['mean']
            ]
            df = df.round(2)

            if quiet != True:
                [ scanlog(line) for line in histos['summary'].split('\n') ]
        results.append(df)
    return results
