# app.bnc.market
"""
GLOBALS REFERENCED
dfc

"""

#-----------------------------------------------------------------------------
def agg_change(dfc):
    """
    """
    labels = ['5 min', '1 hr', '4 hrs', '12 hrs', '24 hrs']
    row_label = 'Agg.Price'

    _list = [
        agg_pct_change(dfc, '1m', span=5, label='5 min'),
        agg_pct_change(dfc, '1m', span=60, label='1 hr'),
        agg_pct_change(dfc, '1h', span=4, label='4 hr'),
        agg_pct_changee(dfc, '1h', span=12, label='12 hr'),
        agg_pct_change(dfc, '1h', span=24, label='24 hr')
    ]

    df = pd.DataFrame(
        {labels[n]:_list[n] for n in range(0,len(labels))},
        index=[row_label])
    df = df[labels]

    # Print values to % str
    for n in range(0,len(labels)):
        value = df[df.columns[n]][0]
        df.set_value(row_label, df.columns[n], "{:+,.2f}%".format(value))
    return df

#-----------------------------------------------------------------------------
def agg_change(dfc, freq_str, span=None, label=None):
    """Aggregate percent market change in last 1 minute.
    # FIXME: only pull up to date candle data. slice by date
    # to make sure no other data is distorting the results.
    """
    freq = strtofreq[freq_str]

    span = span if span else 2
    group = dfc.xs(freq, level=1).groupby('pair')

    if span == 2:
        pct_mkt = group.tail(2).groupby('pair').pct_change().mean()
    elif span > 2:
        pct_mkt = group.tail(span).dropna().groupby('pair').agg(
            lambda x: ((x.iloc[-1] - x.iloc[0]) / x.iloc[0])*100
        ).mean()

    df = pd.DataFrame(pct_mkt, columns=[label]).round(4)[0:1]
    df.index = ['Pct Change']
    return df
