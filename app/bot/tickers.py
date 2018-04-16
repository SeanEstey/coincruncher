# app.bot.tickers
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from pymongo import ReplaceOne
from pprint import pprint
from binance.client import Client
import app, app.bot
from app.common.utils import utc_datetime as now
log = logging.getLogger('tickers')
def scanlog(msg): log.log(98, msg)

#------------------------------------------------------------------------------
def aggregate_mkt(freqstr=None):
    formatters={
        '24h_wt_price_change': '{:+.2f}%'.format,
        '24h_agg_volume': '{:,.0f}'.format
    }
    try:
        df = binance_24h()
    except Exception as e:
        return print("Binance client error. {}".format(str(e)))

    vol = df.groupby('quoteAsset').apply(lambda x: x['quoteVol'].sum())
    vol.name = 'volume'
    dfV = pd.DataFrame(vol)

    summaries=[]
    for idx, row in dfV.iterrows():
        summaries.append(summarize(df, idx))

    df_agg = pd.DataFrame(summaries)
    df_agg.index = df_agg['symbol']
    df_agg = df_agg[['pairs', '24h_wt_price_change', '24h_agg_volume']]
    df_agg = df_agg.round(2).sort_values('24h_agg_volume')

    # Find price change of given frequency by finding difference
    # between both 24h_wt_price_changes
    if freqstr:
        db = app.get_db()
        last = list(db.tickers.find(
            {'freq':freqstr},
            {'_id':0,'freq':0,'ex':0,'time':0}).sort('time',-1))

        db.tickers.insert_one({**{'ex':'Binance', 'time': now(), 'freq': freqstr},
            **df_agg.to_dict('index')})

        if len(last) > 0:
            _df = pd.DataFrame(last[0]).T
            k = '{}_wt_price_change'.format(freqstr)
            df_agg[k] = df_agg['24h_wt_price_change'] - _df['24h_wt_price_change']
            formatters[k] = '{:+.2f}%'.format

    scanlog("")
    scanlog("Aggregate Markets")
    lines = df_agg.to_string(formatters=formatters).split("\n")
    [ scanlog(line) for line in lines]
    return df_agg

#------------------------------------------------------------------------------
def summarize(df, symbol):
    # Filter rows
    df = df[df.index.str.contains(symbol)]

    # Calc volume for both lhs/rhs symbol pairs.
    _df = df.copy()
    for idx, row in _df[_df.index.str.startswith(symbol)].iterrows():
        tmp = row['quoteVol']
        _df.ix[idx,'quoteVol'] = row['volume']
        _df.ix[idx,'volume'] = tmp
    wt_price_change = \
        (_df['pctPriceChange'] * _df['quoteVol']).sum() / _df['quoteVol'].sum()

    #print("{} Stats: {} pairs, {:+.2f}% weighted price change, "\
    #    "{:,.1f} {} traded in last 24 hours."\
    #    .format(symbol, len(df), wt_price_change, _df['quoteVol'].sum(),
    #    symbol))

    return {
        'symbol': symbol,
        'pairs': len(df),
        '24h_wt_price_change': wt_price_change,
        '24h_agg_volume': _df['quoteVol'].sum()
    }

#------------------------------------------------------------------------------
def binance_24h():
    from_ts = datetime.fromtimestamp

    try:
        client = Client("","")
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
    df['openTime'] = df['openTime'].apply(lambda x: from_ts(int(x/1000)))
    df['closeTime'] = df['closeTime'].apply(lambda x: from_ts(int(x/1000)))
    df = df.sort_index()
    df = df.rename(columns={
        'priceChangePercent':'pctPriceChange',
        'quoteVolume':'quoteVol'
    })

    # Query metadata, identity quote assets for all pairs
    meta = app.get_db().meta.find()
    dfM = pd.DataFrame(list(meta))
    dfM.index = dfM['symbol']
    dfM = dfM[['baseAsset', 'quoteAsset']]
    df = df.join(dfM)
    return df.sort_index()

#------------------------------------------------------------------------------
def meta():
    # Lookup/store Binance asset metadata for active trading pairs.
    client = Client('','')
    info = client.get_exchange_info()
    symbols = info['symbols']
    ops = [ReplaceOne({'symbol':n['symbol']}, n, upsert=True) for n in symbols]
    app.get_db().meta.bulk_write(ops)
    print("{} exchange symbols metadata updated.".format(len(symbols)))
