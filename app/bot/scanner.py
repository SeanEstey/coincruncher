# app.bot.scanner
import logging
import pytz
import pandas as pd
import numpy as np
from binance.client import Client
from docs.botconf import macd_scan, tradepairs
import app, app.bot
from app.common.utils import to_local
from app.common.timeutils import strtofreq
from . import candles, macd, tickers, trade
log = logging.getLogger('scanner')
def scanlog(msg): log.log(98, msg)

#------------------------------------------------------------------------------
def run():
    scanlog('*'*59)
    try:
        dfT = tickers.binance_24h().sort_values('24hPriceChange')
        dfA = tickers.aggregate_mkt()
    except Exception as e:
        return print("Agg/Ticker Binance client error. {}".format(str(e)))

    authlist=[]

    for pair in dfT.index.tolist():
        # Ticker/Aggregate Market filters
        q_asset = dfT.loc[pair]['quoteAsset']
        mkt = dfA.loc[q_asset]
        tckr = dfT.loc[pair]

        results = [ fn(tckr, mkt) for fn in tradepairs['filters'] ]

        if all(result == True for result in results):
            print("{} passed filter".format(pair))
            authlist.append({'pair':pair, 'freq':'30m'})
        else:
            #print("{} failed filter: {}".format(pair, results))
            continue

        for rng in macd_scan:
            freqstr, startstr, periods = rng['freqstr'], rng['startstr'], rng['periods']
            freq = strtofreq(freqstr)

            try:
                candles.update([pair], freqstr, start=startstr, force=True)
            except Exception as e:
                return print("Binance client error. {}".format(str(e)))
            else:
                df = candles.load([pair], freqstr=freqstr, startstr=startstr)
                df = df.loc[pair, freq]

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

    print("authlist.length={}".format(len(authlist)))
    trade.enable_pairs(authlist)
