# app.bot.scanner
import logging
import time
import pytz
import pandas as pd
import numpy as np
from binance.client import Client
from docs.botconf import *
import app, app.bot
from app.common.timer import Timer
from app.common.utils import to_local, utc_datetime as now
from app.common.timeutils import strtofreq
from . import candles, macd, tickers, trade
def scanlog(msg): log.log(98, msg)

log = logging.getLogger('scanner')
client = None

#---------------------------------------------------------------------------
def run():
    global client
    client = Client('','')
    update()
    tmr = Timer(name='scanner', expire='every 30 clock min utc')

    while True:
        if tmr.remain() == 0:
            update()
            tmr.reset()
        time.sleep(300)

#------------------------------------------------------------------------------
def update():
    from app.common.utils import strtodt, strtoms
    global client
    if client is None:
        client = Client('','')

    scanlog('*'*59)

    try:
        dfT = tickers.binance_24h(client).sort_values('24hPriceChange')
        dfA = tickers.aggregate_mkt(client)
    except Exception as e:
        return print("Agg/Ticker Binance client error. {}".format(str(e)))

    authpairs=[]
    for pair in dfT.index.tolist():
        q_asset = dfT.loc[pair]['quoteAsset']
        mkt = dfA.loc[q_asset]
        tckr = dfT.loc[pair]
        results = [fn(tckr, mkt) for fn in TRADE_PAIR_ALGO['filters']]
        if any([n == False for n in results]):
            continue
        else:
            authpairs.append(pair)

    print("{} pairs authed: {}".format(len(authpairs), authpairs))
    print("app.bot.dfc.length={}".format(len(app.bot.dfc)))

    # Load revelent historic candle data from DB, query any (pair,freq)
    # index data that's missing.
    app.bot.dfc = candles.load(authpairs, TRADEFREQS, dfm=app.bot.dfc)  #pd.DataFrame())
    tuples = pd.MultiIndex.from_product([authpairs, TRADEFREQS]).values.tolist()
    for idx in tuples:
        if (idx[0], strtofreq(idx[1])) in app.bot.dfc.index:
            continue
        print("Retrieving {} candle data...".format(idx))
        candles.update([idx[0]], [idx[1]], client=client)
        app.bot.dfc = candles.load([idx[0]], [idx[1]], dfm=app.bot.dfc)

    for pair in authpairs:
        for freqstr in TRADEFREQS:
            freq = strtofreq(freqstr)
            periods = int((strtoms("now utc") - strtoms(DEF_KLINE_HIST_LEN)) / ((freq * 1000)/2))
            df = app.bot.dfc.loc[pair,freq]
            dfh, phases = macd.histo_phases(df, pair, freqstr, periods)
            dfh = dfh.tail(3)
            idx = dfh.index.to_pydatetime()
            dfh.index = [ to_local(n.replace(tzinfo=pytz.utc)).strftime("%m-%d %H:%M") for n in idx]
            dfh = dfh.rename(columns={
                'ampMean':'amp.mean',
                'ampMax':'amp.max',
                'priceY':'price.y',
                'priceX':'price.x'
            })
            scanlog("{} {} Macd Phases".format(pair, freqstr))
            lines = dfh.to_string(
                columns=['bars','amp.mean','amp.max','price.y','price.x','capt'],
                formatters={
                    'bars':      ' {:} '.format,
                    'amp.mean':  ' {:+.2f}'.format,
                    'amp.max':   ' {:+.2f}'.format,
                    'price.y':   '  {:+.2f}%'.format,
                    'price.x':   '  {:+.2f}%'.format,
                    'capt':      '  {:.2f}'.format
                }
            ).split("\n")
            [ scanlog(line) for line in lines]
            scanlog("")

    print("authpairs.length={}".format(len(authpairs)))
    trade.enable_pairs(authpairs)


"""
try:
    candles.update([pair], [freqstr], startstr=startstr, client=client)
except Exception as e:
    return print("Binance client error. {}".format(str(e)))
else:
    df = candles.load([pair], [freqstr], startstr=startstr)
    df = df.loc[pair, freq]
"""
