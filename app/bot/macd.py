# app.bot.macd
import logging
from pprint import pprint
from datetime import timedelta as delta, datetime
from dateparser import parse
import pytz
import numpy as np
import pandas as pd
from docs.conf import macd_ema
from app.common.utils import utc_datetime as now, to_relative_str as relative
from app.common.utils import pct_diff, to_local, abc
from app.common.timeutils import strtofreq, freqtostr
from app.common.timer import Timer
import app.bot

log = logging.getLogger('macd')

#-----------------------------------------------------------------------------
def generate(df, ema=None, normalize=True):
    """Append normalized macd histo column to given dataframe.
    Normalized values in range(-1,1).
    # TODO: decide whether to leave "min_periods=_ema[1] - 1"
    """
    df = df.copy()
    _ema = ema if ema else macd_ema

    fast = df['close'].ewm(span=_ema[0], adjust=True, ignore_na=False, min_periods=_ema[1]
        ).mean()
    fast.name = 'fast'
    slow = df['close'].ewm(span=_ema[1], adjust=True, ignore_na=False, min_periods=_ema[1]
        ).mean()
    slow.name='slow'

    macd = pd.Series(fast - slow, name='macd')
    signal = macd.ewm(span=_ema[2], adjust=True, ignore_na=False, min_periods=_ema[1]
        ).mean()
    signal.name='signal'

    df = df.join(pd.DataFrame(fast))
    df = df.join(pd.DataFrame(slow))
    df = df.join(pd.DataFrame(macd))
    df = df.join(pd.DataFrame(signal))

    # Probably a more efficient way to do this transformation.
    histo = pd.Series(macd - signal, name='macd_diff')
    if normalize:
        pos = pd.DataFrame(histo[histo >= 0])
        neg = pd.DataFrame(abs(histo[histo < 0]))
        norm_histo = \
            ((pos-pos.min()) / (pos.max()-pos.min())).append(
            ((neg-neg.min()) / (neg.max()-neg.min()))*-1)
        df = df.join(norm_histo)
    else:
        df = df.join(pd.DataFrame(histo))
    return df

#------------------------------------------------------------------------------
def histo_phases(df, pair, freqstr, periods, to_bson=False):
    """Groups and analyzes the MACD histogram phases within given timespan.
    Determines how closely the histogram bars track with price.
    """
    DF = pd.DataFrame
    freq = strtofreq(freqstr)
    dfmacd = generate(df).tail(periods)['macd_diff']
    np_arr, descs, phases = [],[],[]
    idx = 0

    while idx < len(dfmacd):
        iloc, row, phase, desc = next_phase(dfmacd, freq, idx) ##.values()
        dfmacd = dfmacd.drop_duplicates()
        if row is None:
            idx+=1
            continue
        else:
            np_arr.append(row)
            descs.append(desc)
            phases.append(phase)
            idx = iloc[1] + 1

    dfh = DF(np_arr, columns=['start', 'end', 'bars', 'phase', 'ampMean', 'ampMax'])

    # Gen labels and calc % price changes
    lbls, pxy_corr, pct_py, pct_px = [],[],[],[]
    j=0
    for i in range(0, len(dfh)):
        if j == len(abc):
            j=0
        lbls.append("{} ({})".format(abc[j].upper(), dfh.iloc[i]['phase']))
        j+=1

    # Determine correlation between histogram bars and price movement
    # Find overall histogram=>candle close correlation
    for i in range(0, len(dfh)):
        _slice = df.loc[slice(dfh.iloc[i]['start'], dfh.iloc[i]['end'])]

        pct_py.append(pct_diff(_slice['low'].min(), _slice['high'].max()))
        pct_px.append(pct_diff(_slice.iloc[0]['open'], _slice.iloc[-1]['close']))

        if len(phases[i]) == len(_slice):
            pxy_corr.append(phases[i].corr(_slice['close']))
        else:
            pxy_corr.append(np.nan)

    dfh['lbl'] = lbls
    dfh['duration'] = dfh['end'] - dfh['start']
    dfh['priceY'] = pct_py
    dfh['priceX'] = pct_px
    dfh['capt'] = abs(dfh['priceX'] / dfh['priceY'])
    dfh['corr'] = pxy_corr

    # Append cols/clean up formatting
    dfh.index = dfh['start']

    dfh = dfh.sort_index()
    dfh = dfh[['lbl', 'bars', 'duration', 'ampMean', 'ampMax',
        'priceY', 'priceX', 'capt', 'corr']].round(2)

    if to_bson:
        dfh = dfh.reset_index()
        dfh['start'] = [str(to_local(n.to_pydatetime().replace(tzinfo=pytz.utc))) for n in dfh['start']]
        dfh['duration'] = dfh['duration'].apply(lambda x: str(x.to_pytimedelta()))
        dfh['bars'].astype('int')

        phases[-1] = phases[-1].round(3)
        idx = phases[-1].index.to_pydatetime()
        phases[-1].index = [str(to_local(n.replace(tzinfo=pytz.utc))) for n in idx]

    return (dfh, phases)

#------------------------------------------------------------------------------
def next_phase(dfmacd, freq, start_idx):
    diff = dfmacd.iloc[start_idx]

    try:
        if diff > 0:
            signs = ('positive', '+')
            skip = dfmacd.iloc[start_idx:][dfmacd < 0].head(1).index
        elif diff < 0:
            signs = ('negative', '-')
            skip = dfmacd.iloc[start_idx:][dfmacd > 0].head(1).index
        else:
            return (None,None,None,None)
    except Exception as e:
        log.debug("Duplicate indices!")
        log.debug(dfmacd[dfmacd.index.duplicated()])
        return (None,None,None,None)

    end_idx = len(dfmacd)-1 if skip.empty else dfmacd.index.get_loc(skip[0])-1
    n_bars = end_idx - start_idx + 1
    phase = dfmacd.iloc[start_idx:end_idx+1].drop_duplicates() #.copy()

    dt1 = phase.head(1).index[0].to_pydatetime()
    local_dt1 = to_local(dt1.replace(tzinfo=pytz.utc))
    dt2 = phase.tail(1).index[0].to_pydatetime()
    local_dt2 = to_local(dt2.replace(tzinfo=pytz.utc))

    return (
        (start_idx, end_idx),
        np.array([
            dt1, dt2, n_bars, signs[1], phase.mean(), phase.max()
        ]),
        phase,
        '{0:%b %d}-{1:%d} @ {0:%H:%m}-{1:%H:%m}: '\
        '{2:} phase, {3:} bars x {4:.2f} amp.'\
        .format(local_dt1, local_dt2, signs[1], n_bars, abs(phase.mean()))
    )

#------------------------------------------------------------------------------
def plot(pair, freqstr, periods):
    """
    # fig['layout']['xaxis1'].update(titlefont=dict(
    #    family='Arial, sans-serif',
    #    size=18,
    #    color='grey'
    #))
    """
    import pytz
    import plotly.offline as offline
    import plotly.tools as tools, plotly.graph_objs as go
    from . import candles

    freq = strtofreq(freqstr)
    strunit=None
    if freqstr[-1] == 'm':
        strunit = 'minutes'
    elif freqstr[-1] == 'h':
        strunit = 'hours'
    elif freqstr[-1] == 'd':
        strunit = 'days'
    n = int(freqstr[:-1])
    startstr = "{} {} ago utc".format(n * periods + 25, strunit)

    # Query/load candle data
    candles.update([pair], freqstr, start=startstr, force=True)
    df = candles.bulk_load([pair], [freqstr], startstr=startstr)
    df = df.loc[pair,freq]
    # Macd analysis
    dfmacd = generate(df)
    aggdesc = agg_describe(df, pair, freqstr, periods)

    aggdesc['summary'] = aggdesc['summary'].replace("\n", "<br>")

    # Stacked Subplots with a Shared X-Axis
    t1 = go.Scatter(
        x=dfmacd.index,
        y=dfmacd['close'],
        name="Price")
    t2 = go.Bar(
        x=dfmacd.index,
        y=dfmacd['macd_diff'],
        name="MACD_diff (normalized)",
        yaxis='y2')
    t3 = go.Bar(
        x=dfmacd.index,
        y=dfmacd['volume'],
        name="Volume",
        yaxis='y3')
    data = [t1, t2, t3]

    annotations = []
    trades = app.get_db().trades.find(
        {'pair':pair, 'freq':freqstr, 'status':'closed', 'start_time':{'$gte':parse(startstr)}})

    yoffset=40

    for trade in trades:
        row_a = dfmacd[dfmacd.index <= trade['start_time']].iloc[-1]
        row_b = dfmacd[dfmacd.index <= trade['end_time']].iloc[-1]

        annotations.append(dict(
            x=row_a.name.to_pydatetime().replace(tzinfo=pytz.utc),
            y=row_a['close'],
            xref='x',
            yref='y',
            text='{} entry'.format(trade['strategy']),
            showarrow=True,
            arrowhead=7,
            ax=0,
            ay=yoffset
        ))
        yoffset-=10
        annotations.append(dict(
            x=row_b.name.to_pydatetime().replace(tzinfo=pytz.utc),
            y=row_b['close'],
            xref='x',
            yref='y',
            text='{} exit'.format(trade['strategy']),
            showarrow=True,
            arrowhead=7,
            ax=0,
            ay=yoffset
        ))
        yoffset-=10

    print("{} annotations".format(len(annotations)))

    layout = go.Layout(
        title='{} {} Trade Summary (24 Hours)'.format(pair, freqstr),
        margin = dict(l=100, r=100, b=400, t=75, pad=25),
        xaxis=dict(
            anchor = "y3",
            #domain=[0.0, 0.1],
            title="<BR>" + aggdesc['summary']
        ),
        yaxis=dict(
            domain=[0.4, 1]
        ),
        yaxis2=dict(
            domain=[0.2, 0.4]
        ),
        yaxis3=dict(
            domain=[0, 0.2]
        ),
        annotations=annotations
    )

    fig = go.Figure(data=data, layout=layout)
    return fig


    """
    stats = {
        'pair':pair,
        'freqstr':freqstr,
        'periods':len(dfmacd),
        'phases': len(phases)
    }
    stats[sign] = {
        'n_phases': len(grp),
        'mean_amplitude': amplitude.mean(),
        'price_diff':{
            'sum': price_diff.sum(),
            'mean': price_diff.mean()
        },
        'area': pd.DataFrame(area).describe().to_dict(),
        'periods': pd.DataFrame(periods).describe().to_dict(),
        'duration': {
            'sum': relative(delta(seconds=int(duration.sum()))),
            'mean': relative(delta(seconds=int(duration.mean())))
        }
    }
    return {
        'summary':summary,
        'stats':stats,
        'phases':phases,
        'elapsed_ms':t1.elapsed()
    }
    """
    # print("{} MACD Histogram Phases".format(pair))
    # for i in range(0, len(descs)):
    #    [print("{}. {}".format(abc[i].upper(), descs[i]))]

#------------------------------------------------------------------------------
def describe(candle, ema=None):
    """Describe current histo phase.
    """
    _ema = ema if ema else macd_ema
    df = app.bot.dfc.loc[candle['pair'], strtofreq(candle['freq'])].copy()
    macd = generate(df, ema=_ema)

    # Isolate current histo phase
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
def agg_histo_phases(pairs, freqstr, startstr, periods):
    df = pd.DataFrame(df[columns].values,
        index = pd.MultiIndex.from_arrays(
            [df['pair'], df['freq'], df['open_time']],
            names = ['pair','freq','open_time']),
        columns = columns
    ).sort_index()

