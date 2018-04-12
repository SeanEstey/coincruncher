# app.bot.macd
import logging
from datetime import timedelta as delta, datetime
import numpy as np
import pandas as pd
from docs.conf import macd_ema
from app import strtofreq
from app.common.timer import Timer
import app.bot
from app.bot import pct_diff

log = logging.getLogger('macd')

#-----------------------------------------------------------------------------
def generate(df, ema=None, normalize=True):
    """Append normalized macd oscilator column to given dataframe.
    Normalized values in range(-1,1).
    """
    _ema = ema if ema else macd_ema

    fast = df['close'].ewm(
        span=_ema[0],
        #min_periods=_ema[1] - 1,
        adjust=True,
        ignore_na=False
    ).mean()
    fast.name = 'fast'

    slow = df['close'].ewm(
        span=_ema[1],
        #min_periods=_ema[1] - 1,
        adjust=True,
        ignore_na=False
    ).mean()
    slow.name='slow'

    macd = pd.Series(fast - slow, name='macd')

    signal = macd.ewm(
        span=_ema[2],
        #min_periods=_ema[1] - 1,
        adjust=True,
        ignore_na=False
    ).mean()
    signal.name='signal'

    df = df.join(fast)
    df = df.join(slow)
    df = df.join(macd)
    df = df.join(signal)

    oscilator = pd.Series(macd - signal, name='macd_diff')

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
def describe(candle, ema=None):
    """Describe current oscilator phase.
    """
    _ema = ema if ema else macd_ema

    df = app.bot.dfc.loc[candle['pair'], strtofreq[candle['freq']]]
    macd = generate(df, ema=_ema)

    # Isolate current oscilator phase
    last = np.float64(macd.tail(1)['macd_diff'])
    if last < 0:
        if len(macd[macd['macd_diff'] > 0]) > 0:
            marker = macd[macd['macd_diff'] > 0].iloc[-1]
        else:
            marker = macd['macd_diff'].tail(1) #.iloc[-1]
    else:
        if len(macd[macd['macd_diff'] < 0]) > 0:
            marker = macd[macd['macd_diff'] < 0].iloc[-1]
        else:
            marker = macd['macd_diff'].tail(1) #.iloc[-1]

    try:
        phase = macd.loc[slice(marker.name, macd.iloc[-1].name)].iloc[1:]['macd_diff']
        desc = phase.describe()
    except Exception as e:
        phase = macd.iloc[1:]['macd_diff']
        desc = phase.describe()

    if len(phase) == 1:
        details = 'Oscilator phase change.\n'
        trend = 'UPWARD' if phase.iloc[0] >= 0 else 'DOWNWARD'
    else:
        trend = 'UPWARD' if last > phase.iloc[-2] else 'DOWNWARD'

        if last < 0:
            #if last > desc['min']:
            #    pct = app.bot.pct_diff(desc['min'], last)
            details = 'MACD (-), {0} bottom, trending {1}.\n'\
                'Bottom is {2:+.2f}, mean is {3:+.2f}, now at {4:+.2f}.\n'\
                .format(
                    'ABOVE' if last > desc['min'] else 'AT',
                    trend,
                    float(desc['min']),
                    float(desc['mean']),
                    float(last))
        elif last >= 0:
            details = 'MACD (+), {0} peak, trending {1}.\n'\
                'Peak is {2:+.2f}, mean is {3:+.2f}, now at {4:+.2f}.\n'\
                .format(
                    'BELOW' if last < desc['max'] else 'AT',
                    trend,
                    float(desc['max']),
                    float(desc['mean']),
                    float(last))

    return {'phase':phase, 'trend':trend, 'details':details}

#------------------------------------------------------------------------------
def agg_describe(pair, freqstr, n_periods, pdfreqstr=None):
    """Describe aggregate macd positive/negative oscilator phases in timespan.
    """
    t1 = Timer()
    from app.common.utils import to_relative_str as relative
    freq = strtofreq[freqstr]

    df_macd = generate(
        app.bot.dfc.loc[pair, freq]
    ).dropna().tail(n_periods).asfreq(pdfreqstr)

    phases=[]
    last_iloc = 0

    while last_iloc <= len(df_macd) - 1:
        phase = _get_phase(df_macd, last_iloc+1)
        if not phase:
            break
        phases.append(phase)
        last_iloc = phase['iloc'][1]

    stats = {
        'pair':pair,
        'freqstr':freqstr,
        'periods':len(df_macd['macd_diff']),
        'phases': len(phases)
    }

    #"{} MACD Phase Analysis\n"\
    summary = "\n"\
        "{} Freq: {}, Periods: {}, Total Phases: {}\n"\
        .format(
            pair,
            freqstr, len(df_macd['macd_diff']), len(phases))

    for sign in ['POSITIVE', 'NEGATIVE']:
        grp = [ n for n in phases if n['sign'] == sign ]

        area = np.array([ n['area'] for n in grp ])
        if len(area) == 0:
            area = np.array([0])

        periods = np.array([ n['length'] for n in grp ])
        if len(periods) == 0:
            periods = np.array([0])

        duration = np.array([ n['seconds'] for n in grp])
        if len(duration) == 0:
            duration = np.array([0])

        price_diff = np.array([
            pct_diff(
                n['df'].iloc[0]['close'],
                n['df'].iloc[-1]['close']
            ) for n in grp
        ])

        #"\tArea: {:.2f} (mean: {:.2f})\n"\
        #"\tPeriods: {:} (mean: {:.2f})\n"\
        summary += \
            "\t({}) phases: {}\n"\
            "\tPrice: {:+.2f}% (mean: {:+.2f}%)\n"\
            "\tDuration: {:} (mean: {})\n"\
            .format(
                '+' if sign == 'POSITIVE' else '-',
                len(grp),
                price_diff.sum(),
                price_diff.mean() if len(price_diff) > 0 else 0,
                #abs(area.sum()), abs(area.mean()),
                #periods.sum(), periods.mean(),
                relative(delta(seconds=int(duration.sum()))),
                relative(delta(seconds=int(duration.mean())))
            )

        stats[sign] = {
            'n_phases': len(grp),
            'price_diff':{
                'sum': price_diff.sum(),
                'mean': price_diff.mean()
            },
            'area': pd.DataFrame(area).describe().to_dict(),
            'periods': pd.DataFrame(periods).describe().to_dict(),
            'duration': pd.DataFrame(duration).describe().to_dict()
        }

    return {
        'summary':summary,
        'stats':stats,
        'phases':phases,
        'elapsed_ms':t1.elapsed()
    }

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

#------------------------------------------------------------------------------
def plot(pair, units, n_units, n_periods):
    """
    """
    from dateparser import parse
    import plotly.offline as offline
    import plotly.tools as tools, plotly.graph_objs as go
    from app.common.utils import utc_datetime as now
    from . import candles

    freqstr = ('%s%s'%(n_units, units[1]), '%s%s'%(n_units, units[2]))
    freq = strtofreq[freqstr[0]]
    start_str = "{} {} ago utc".format((n_periods + 75) * n_units, units[0])

    candles.update([pair], freqstr[0],
        start=start_str, force=True)
    app.bot.dfc = candles.merge_new(pd.DataFrame(), [pair],
        span=now()-parse(start_str))
    df_macd = generate(app.bot.dfc.loc[pair, freq])
    scan = agg_describe(pair, freqstr[0], n_periods, pdfreqstr=freqstr[1])
    scan['summary'] = scan['summary'].replace("\n", "<br>")

    # Stacked Subplots with a Shared X-Axis
    t1 = go.Scatter(
        x=df_macd.index,
        y=df_macd['close'],
        name="Price")
    t2 = go.Bar(
        x=df_macd.index,
        y=df_macd['macd_diff'],
        name="MACD_diff (normalized)",
        yaxis='y2')
    t3 = go.Bar(
        x=df_macd.index,
        y=df_macd['volume'],
        name="Volume",
        yaxis='y3')
    data = [t1, t2, t3]

    layout = go.Layout(
        title='{} MACD'.format(pair),
        margin = dict(l=100, r=100, b=400, t=75, pad=25),
        xaxis=dict(
            anchor = "y3",
            #domain=[0.0, 0.1],
            title="<BR>" + scan['summary']
        ),
        yaxis=dict(
            domain=[0.4, 1]
        ),
        yaxis2=dict(
            domain=[0.2, 0.4]
        ),
        yaxis3=dict(
            domain=[0, 0.2]
        )
        #fig['layout']['xaxis1'].update(titlefont=dict(
        #    family='Arial, sans-serif',
        #    size=18,
        #    color='grey'
        #))
    )

    fig = go.Figure(data=data, layout=layout)
    return fig


