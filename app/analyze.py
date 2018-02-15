# app.analyze

import logging
from pprint import pprint
import pandas as pd
from app import get_db
from app.timer import Timer
from app.utils import parse_period, utc_dtdate
log = logging.getLogger('analyze')

#------------------------------------------------------------------------------
def top_symbols(rank):
    """Get list of ticker symbols within given rank.
    """
    db = get_db()
    _date = list(db.tickers_5m.find().sort("date",-1).limit(1))[0]["date"]
    cursor = db.tickers_5m.find({"date":_date, "rank":{"$lte":rank}}).sort("rank",1)
    return [n["symbol"] for n in list(cursor)]

#------------------------------------------------------------------------------
def corr(symbols, start, end):
    """Generate price correlation matrix for given list of symbols.
    """
    db = get_db()
    t1 = Timer()

    cursor = db.tickers_1d.aggregate([
        {"$match":{"date":{"$gte":start, "$lt":end}}},
        {"$group":{
            "_id":"$symbol",
            "date":{"$push":"$date"},
            "price":{"$push":"$close"}
        }}
    ])

    t_aggr = t1.clock(t='ms')
    t1.restart()
    df = pd.DataFrame(list(cursor))
    df.index = df["_id"]
    t_df = t1.clock(t='ms')
    t1.restart()

    log.debug("queried %s results in %s ms.", t_aggr, len(df))

    big_df = pd.DataFrame(
        columns=[symbols[0]],
        index=df.loc[symbols[0]]["date"],
        data=df.loc[symbols[0]]["price"]
    ).sort_index()

    for sym in symbols[1:]:
        if sym not in df.index:
            continue
        big_df = big_df.join(
            pd.DataFrame(
                columns=[sym],
                index=df.loc[sym]["date"],
                data=df.loc[sym]["price"]
            ).sort_index()
        )

    big_df = big_df[::-1]
    s1 = len(big_df)
    big_df = big_df.dropna().drop_duplicates()
    s2 = len(big_df)
    log.debug("df size pre-dropna=%s, post=%s", s1, s2)
    corr = big_df.corr().round(2)
    #log.debug("concat + corr calculated in %s ms", t1.clock(t='ms'))
    return corr
    #return big_df

#------------------------------------------------------------------------------
def corr_minmax(symbol, start, end, max_rank):
    """Find lowest & highest price correlation symbols (within max_rank) with
    given ticker symbol.
    """
    db = get_db()
    symbols = top_symbols(max_rank)
    df = corr(symbols, start, end)

    log.debug("df.length=%s", len(df))

    col = df[symbol]
    del col[symbol]
    df = df.dropna()

    if len(df) < 1:
        return {"min":None,"max":None}

    #df = df.round(2)
    col = col.round(2)

    return {
        "symbol":symbol,
        "start":start,
        "end":end,
        "corr":col,
        "min": {col.idxmin(): col[col.idxmin()]},
        "max": {col.idxmax(): col[col.idxmax()]}
    }

#------------------------------------------------------------------------------
def corr_minmax_history(symbol, start, freq, max_rank):
    delta = parse_period(freq)[2]
    _date = start
    results=[]
    while _date < utc_dtdate():
        results.append(corr_minmax(symbol, _date, _date+delta, max_rank))
        _date += delta

    return results
