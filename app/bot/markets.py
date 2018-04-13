import logging
import pandas as pd
import app.bot
from app.common.timeutils import strtofreq
log = logging.getLogger('bot.markets')

#-----------------------------------------------------------------------------
def agg_pct_change(freq_str, span=None, label=None):
    """Aggregate percent market change in last 1 minute.
    # FIXME: only pull up to date candle data. slice by date
    # to make sure no other data is distorting the results.
    """
    dfc = app.bot.dfc
    freq = strtofreq(freq_str)
    label = label if label else ''

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
