# app.bot.macd
import logging
import numpy as np
import pandas as pd
from docs.rules import STRATS
from app import strtofreq
from app.utils.timer import Timer
import app.bot
from app.bot import pct_diff

log = logging.getLogger('macd')
rules = STRATS['macd']

#-----------------------------------------------------------------------------
def oscilator(df, fast_span=None, slow_span=None, normalize=True):
    """Append normalized macd oscilator column to given dataframe.
    Normalized values in range(-1,1).
    """
    n_fast = fast_span if fast_span else rules['fast_span']
    n_slow = slow_span if slow_span else rules['slow_span']

    ema_fast = df['close'].ewm(
        span=n_fast,
        min_periods=n_slow - 1,
        adjust=True,
        ignore_na=False
    ).mean()

    ema_slow = df['close'].ewm(
        span=n_slow,
        min_periods=n_slow - 1,
        adjust=True,
        ignore_na=False
    ).mean()

    macd = pd.Series(ema_fast - ema_slow)

    macd_sign = macd.ewm(
        span=9,
        min_periods=8,
        adjust=True,
        ignore_na=False
    ).mean()

    oscilator = pd.Series(macd - macd_sign, name='macd_diff')

    # Probably a more efficient way to do this transformation.
    if normalize:
        pos = pd.DataFrame(oscilator[oscilator >= 0])
        neg = pd.DataFrame(abs(oscilator[oscilator < 0]))
        norm_oscilator = \
            ((pos-pos.min()) / (pos.max()-pos.min())).append(
            ((neg-neg.min()) / (neg.max()-neg.min()))*-1)

        df = df.join(norm_oscilator)
    else:
        df = df.join(oscilator)

    return df

#------------------------------------------------------------------------------
def describe(candle, fast_span=None, slow_span=None):
    """Describe current oscilator phase.
    """
    n_fast = fast_span if fast_span else rules['fast_span']
    n_slow = slow_span if slow_span else rules['slow_span']

    df = app.bot.dfc.loc[candle['pair'], strtofreq[candle['freq']]]
    macd = oscilator(df, fast_span=n_fast, slow_span=n_slow)

    # Isolate current oscilator phase
    last = np.float64(macd.tail(1)['macd_diff'])
    if last < 0:
        marker = macd[macd['macd_diff'] > 0].iloc[-1]
    else:
        marker = macd[macd['macd_diff'] < 0].iloc[-1]

    phase = macd.loc[slice(marker.name, macd.iloc[-1].name)].iloc[1:]['macd_diff']
    desc = phase.describe()

    if len(phase) == 1:
        details = 'Oscilator phase change.'
    else:
        if last < 0:
            #if last > desc['min']:
            #    pct = app.bot.pct_diff(desc['min'], last)
            details = 'MACD negative phase, {0} bottom, trending {1}.\n'\
                'Bottom is {2:+g}, mean is {3:+g}, now at {4:+g}.\n'\
                .format(
                    'ABOVE' if last > desc['min'] else 'AT',
                    'UPWARD' if last > phase.iloc[-2] else 'DOWNWARD',
                    float(desc['min']),
                    float(desc['mean']),
                    float(last))
        elif last >= 0:
            details = 'MACD positive phase, {0} peak, trending {1}.\n'\
                'Peak is {2:+g}, mean is {3:+g}, now at {4:+g}.\n'\
                .format(
                    'BELOW' if last < desc['max'] else 'AT',
                    'UPWARD' if last > phase.iloc[-2] else 'DOWNWARD',
                    float(desc['max']),
                    float(desc['mean']),
                    float(last))

    return {'phase':phase, 'details':details}

#------------------------------------------------------------------------------
def agg_describe(pair, freqstr, n_periods, pdfreqstr=None):
    """Describe all oscilator phases in historical period.
    """
    t1 = Timer()
    from datetime import timedelta as delta
    from app.common.utils import to_relative_str
    freq = strtofreq[freqstr]

    df_macd = oscilator(
        app.bot.dfc.loc[pair, freq],
        strats['macd']['fast_span'],
        strats['macd']['slow_span']
    ).tail(n_periods).asfreq(pdfreqstr)

    phases=[]
    last_iloc = 0

    while last_iloc <= len(df_macd) - 1:
        phase = _get_phase(df_macd, last_iloc+1)
        if not phase:
            break
        phases.append(phase)
        last_iloc = histo['iloc'][1]

    summary = []
    summary.append("\n{} MACD Analysis".format(pair))
    summary.append("Freq: {}, Phases: {}".format(
        df_macd.index.freq.freqstr, len(phases)))

    for sign in ['POSITIVE', 'NEGATIVE']:
        grp = [ n for n in phases if n['sign'] == sign ]

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

        summary.append("{} Oscillations: {}\n"\
            "\tPrice: {:+.2f}%\n"\
            .format(sign.title(), len(grp), price_diff.sum()))
        summary.append("\tTotal_Area: {:.2f}, Mean_Area: {:.2f},\n"\
            "\tTotal_Periods: {:}, Mean_Periods: {:.2f}\n"\
            "\tMean_Duration: {}"\
            .format(
                abs(area.sum()),
                abs(area.mean()),
                periods.sum(),
                periods.mean(),
                mean_duration))
    return {'summary':summary, 'phases':phases, 'elapsed_ms':t1.elapsed()}

#------------------------------------------------------------------------------
def _get_phase(df, start_iloc):
    _df = df.iloc[start_iloc:]

    if start_iloc >= len(df)-1 or _df.iloc[0]['macd_diff'] == np.nan:
        return {}

    if _df.iloc[0]['macd_diff'] >= 0:
        sign = 'POSITIVE'
        next_idx = _df[_df['macd_diff'] < 0].head(1).index
    else:
        sign = 'NEGATIVE'
        next_idx = _df[_df['macd_diff'] >= 0].head(1).index

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
        'area': _df['macd_diff'].sum(),
        'df':_df
    }
