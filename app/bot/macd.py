# app.bot.macd
import logging
from pprint import pprint
from datetime import timedelta as delta, datetime
from dateparser import parse
import pytz
import numpy as np
import pandas as pd
import plotly.graph_objs as go
from docs.conf import *
from docs.botconf import *
from app.common.utils import pct_diff, to_local, abc, strtodt, strtoms
from app.common.timeutils import strtofreq, freqtostr
import app, app.bot
from . import candles, signals

log = logging.getLogger('macd')

#-----------------------------------------------------------------------------
def generate(df, ema=None, normalize=True):
    """Append normalized macd histo column to given dataframe.
    Normalized values in range(-1,1).
    # TODO: decide whether to leave "min_periods=_ema[1] - 1"
    """
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
    df = df.copy()
    dfmacd = generate(df).tail(periods)['macd_diff']
    np_arr, descs, phases = [],[],[]
    idx = 0

    while idx < len(dfmacd):
        try:
            iloc, row, phase, desc = next_phase(dfmacd, freq, idx)
        except Exception as e:
            log.info("{}".format(pair))
            pprint(dfmacd)

        dfmacd = dfmacd#.drop_duplicates()
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

        py = pct_diff(_slice['low'].min(), _slice['high'].max())
        if dfh['ampMean'].iloc[i] < 0:
            py *= -1
        pct_py.append(py)

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

    return (dfh, phases)

#------------------------------------------------------------------------------
def next_phase(dfmacd, freq, start_idx):
    """
    """
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
        log.info(str(e))
        log.info("Duplicate indices!")
        log.info(dfmacd[dfmacd.index.duplicated()])
        dfmacd = dfmacd.tail(-1)
        raise

    end_idx = len(dfmacd)-1 if skip.empty else dfmacd.index.get_loc(skip[0])-1
    n_bars = end_idx - start_idx + 1
    phase = dfmacd.iloc[start_idx:end_idx+1]

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
def plot(pairs=None, freqstr=None, trades=None, startstr=None, indicators=None, normalize=False):
    '''Generate plotly chart html file.
    Stacked Subplots with a Shared X-Axis
    '''
    from app.bot.candles import api_update, bulk_load, bulk_append_dfc
    db = app.db
    startdt = None
    annotations, indicators, traces, indices = [],[],[],[]
    def_start = strtodt(startstr or DEF_KLINE_HIST_LEN)

    if trades is not None:
        indices = [ (n['pair'], strtofreq(n['freqstr'])) for n in trades]
        startdt = min([n['start_time'] for n in trades] + [def_start])
    else:
        indices = [(n, strtofreq(freqstr)) for n in pairs]
        startdt = def_start

    # Price traces.
    for idx in set(indices):
        if idx not in app.bot.dfc.index:
            bulk_load([idx[0]], [freqtostr(idx[1])], startdt=startdt)

            if idx not in app.bot.dfc.index:
                bulk_append_dfc(api_update([idx[0]], [freqtostr(idx[1])]))

        df = app.bot.dfc.ix[idx[0:2]]

        traces.append(go.Scatter(
            x = df.index,
            y = signals.normalize(df['close']) if normalize else df['close'],
            name="{} {}".format(idx[0], freqtostr(idx[1]))
        ))

    # Trade entry/exit annotations
    for trade in trades:
        yoffset=-20
        df = app.bot.dfc.ix[(trade['pair'],strtofreq(trade['freqstr']))]
        df_n = signals.normalize(df['close'])

        for n in [0, -1]:
            ss = trade['snapshots'][n]
            loc = df.index.get_loc(ss['candle']['open_time'].astimezone(pytz.utc))  #.replace(tzinfo=pytz.utc))

            annotations.append(dict(
                x = ss['candle']['open_time'].astimezone(pytz.utc), #tzinfo=pytz.utc),
                y = df_n.iloc[loc],
                xref='x', yref='y',
                text='{} {}'.format(trade['algo'], 'entry' if n == 0 else 'exit'),
                showarrow=True, arrowhead=7, ax=0, ay=yoffset
            ))

    print("{} annotations".format(len(annotations)))


    # Indicators
    for indic in indicators:
        '''
        dfmacd = generate(df)
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
        '''
        pass

    n_div = [len(traces)>0, len(indicators)>0].count(True) #len(annotations)>0].count(True)

    # Setup axes/formatting
    layout = go.Layout(
        title = '{} {} Trade Summary (24 Hours)'\
            .format(pairs, freqstr),
        margin = dict(
            l=100, r=100, b=400, t=75, pad=25
        ),
        xaxis = dict(
            anchor = "y3",
            #domain=[0.0, 0.1],
            title="<BR>"
        ),
        yaxis=dict(
            domain=[1/n_div, 1]
        ),
        annotations=annotations

        #yaxis2=dict(
        #    domain=[0.2, 0.4]
        #),
        #yaxis3=dict(
        #    domain=[0, 0.2]
        #),
    )

    fig = go.Figure(data=traces, layout=layout)
    return fig
