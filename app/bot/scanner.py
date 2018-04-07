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

#------------------------------------------------------------------------------
def macd_analysis(pair, freqstr, n_periods):
    """Calculate trade metrics for MACD oscillators over recent time span.
    """
    freq = strtofreq[freqstr]

    macd = signals.macd(
        app.bot.dfc.loc[pair, freq],
        strats['macd']['fast_span'],
        strats['macd']['slow_span']
    ).tail(n_periods).dropna().asfreq(freqstr.upper())

    histos=[]
    last_iloc = 0

    while last_iloc <= len(macd) - 1:
        histo = get_histo(macd, last_iloc+1)
        if not histo:
            break
        histos.append(histo)
        last_iloc = histo['iloc'][1]

    print("\n{} MACD Analysis".format(pair))
    print("Freq: {}, Phases: {}".format(
        macd.index.freq.freqstr, len(histos)))

    for sign in ['POSITIVE', 'NEGATIVE']:
        grp = [ n for n in histos if n['sign'] == sign ]

        area = np.array([ n['area'] for n in grp ])
        if len(area) == 0:
            area = np.array([0])

        periods = np.array([ n['length'] for n in grp ])
        if len(periods) == 0:
            periods = np.array([0])

        duration = np.array([ n['seconds'] for n in grp])
        if len(duration) > 0:
            mean_duration = to_relative_str(delta(seconds=duration.mean()))
        else:
            mean_duration = 0

        # Total percent gain in area
        price_diff = np.array([
            pct_diff(
                n['df'].iloc[0]['close'],
                n['df'].iloc[-1]['close']
            ) for n in grp
        ])

        print("{} Phases: {}\n"\
            "\tPrice: {:+.2f}%\n"\
            "\tTotal_Area: {:.2f}, Mean_Area: {:.2f},\n"\
            "\tTotal_Periods: {:}, Mean_Periods: {:.2f}\n"\
            "\tMean_Duration: {}"\
            .format(
                sign.title(),
                len(grp),
                price_diff.sum(),
                abs(area.sum()),
                abs(area.mean()),
                periods.sum(),
                periods.mean(),
                mean_duration))
    return histos

#------------------------------------------------------------------------------
def get_histo(df, start_iloc):
    t1 = Timer()
    _df = df.iloc[start_iloc:]

    if start_iloc >= len(df)-1 or _df.iloc[0]['normalized'] == np.nan:
        return {}

    if _df.iloc[0]['normalized'] >= 0:
        sign = 'POSITIVE'
        next_idx = _df[_df['normalized'] < 0].head(1).index
    else:
        sign = 'NEGATIVE'
        next_idx = _df[_df['normalized'] >= 0].head(1).index

    if next_idx.empty:
        next_iloc = len(_df)
    else:
        next_iloc = _df.index.get_loc(next_idx[0])

    _df = _df.iloc[0 : next_iloc]

    return {
        'iloc': (start_iloc, start_iloc + next_iloc - 1),
        'length':len(_df),
        'seconds': (_df.index.freq.nanos / 1000000000) * len(_df),
        'sign':sign,
        'area': _df['normalized'].sum(),
        'df':_df,
        'calc_time_ms': t1.elapsed()
    }
