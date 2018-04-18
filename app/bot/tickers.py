# app.bot.tickers
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from pymongo import ReplaceOne
from pprint import pprint
import app, app.bot
from app.common.utils import utc_datetime as now

log = logging.getLogger('tickers')
def scanlog(msg): log.log(98, msg)

#------------------------------------------------------------------------------
def aggregate_mkt(client, freqstr=None):
    try:
        dfT = binance_24h(client)
    except Exception as e:
        return print("Binance client error. {}".format(str(e)))

    dfV = pd.DataFrame(
        dfT.groupby('quoteAsset').apply(lambda x: x['quoteVol'].sum()),
        columns=['volume'])

    summaries=[]
    for idx, row in dfV.iterrows():
        summaries.append(summarize(dfT, idx))

    dfA = pd.DataFrame(summaries, index=[n['symbol'] for n in summaries])
    dfA = dfA[['pairs', '24hPriceChange', '24hAggVol']]\
        .round(2).sort_values('24hAggVol')

    # Diff in both 24h_delta_price's is freq price change.
    formatters={}
    k=None
    if freqstr:
        db = app.get_db()
        last = list(db.tickers.find({'freq':freqstr},
            {'_id':0,'freq':0,'ex':0,'time':0}).sort('time',-1))
        db.tickers.insert_one({
            **{'ex':'Binance', 'time': now(), 'freq': freqstr},
            **dfA.to_dict('index')
        })

        if len(last) > 0:
            k = '{}.Δprice'.format(freqstr)
            dfA[k] = dfA['24hPriceChange'] - pd.DataFrame(last[0]).T['24hPriceChange']
            formatters[k] = '   {:+.2f}%'.format

    formatters.update({
        '24h.Δprice':   '   {:+.2f}%'.format,
        '24h.agg.vol':  '   {:,.0f}'.format
    })
    scanlog("Aggregate Markets")
    columns=['pairs', k, '24h.Δprice', '24h.agg.vol']
    if not k:
        columns = [n for n in columns if n]
    _cols = dfA.columns.tolist()
    dfA.columns = columns
    lines = dfA.to_string(columns=columns, formatters=formatters).split("\n")
    [scanlog(line) for line in lines]
    scanlog("")
    dfA.columns = _cols
    return dfA

#------------------------------------------------------------------------------
def summarize(df, symbol):
    """
    # print("{} Stats: {} pairs, {:+.2f}% weighted price change, "\
    #    "{:,.1f} {} traded in last 24 hours."\
    #    .format(symbol, len(df), wt_price_change, _df['quoteVol'].sum(),
    #    symbol))
    """
    # Filter rows
    df = df[df.index.str.contains(symbol)]

    # Calc volume for both lhs/rhs symbol pairs.
    _df = df.copy()
    for idx, row in _df[_df.index.str.startswith(symbol)].iterrows():
        tmp = row['quoteVol']
        _df.ix[idx,'quoteVol'] = row['volume']
        _df.ix[idx,'volume'] = tmp
    wt_price_change = \
        (_df['24hPriceChange'] * _df['quoteVol']).sum() / _df['quoteVol'].sum()

    return {
        'symbol': symbol,
        'pairs': len(df),
        '24hPriceChange': wt_price_change,
        '24hAggVol': _df['quoteVol'].sum()
    }

#------------------------------------------------------------------------------
def binance_24h(client):
    try:
        tickers = client.get_ticker()
    except Exception as e:
        raise

    df = pd.DataFrame(tickers)
    df.index = df['symbol']
    # Filter cols
    df = df[['openTime','closeTime','lastPrice','priceChange',
        'priceChangePercent', 'quoteVolume','volume','weightedAvgPrice']]
    # Datatype formatting
    df = df.astype('float64')
    from_ts = datetime.fromtimestamp
    df['openTime'] = df['openTime'].apply(lambda x: from_ts(int(x/1000)))
    df['closeTime'] = df['closeTime'].apply(lambda x: from_ts(int(x/1000)))
    df = df.sort_index()
    df = df.rename(columns={
        'priceChangePercent':'24hPriceChange',
        'quoteVolume':'quoteVol'
    })

    # Query metadata, identity quote assets for all pairs
    meta = app.get_db().assets.find()
    dfM = pd.DataFrame(list(meta))
    dfM.index = dfM['symbol']
    dfM = dfM[['baseAsset', 'quoteAsset']]
    df = df.join(dfM).sort_index()
    # Prune dummy '123456' symbol row
    return df.iloc[1:]
