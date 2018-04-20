# app.bot.scanner
import logging
import time
import pytz
import pandas as pd
import numpy as np
from docs.botconf import *
import app, app.bot
from app.bot import enable_pairs
from app.common.timer import Timer
from app.common.utils import to_local, utc_datetime as now
from app.common.utils import strtodt, strtoms
from app.common.timeutils import strtofreq
from . import candles, macd, tickers, trade
def scanlog(msg): log.log(98, msg)

log = logging.getLogger('scanner')

#---------------------------------------------------------------------------
def run(e_pairs):
    tmr = Timer(name='scanner', expire='every 32 clock min utc')
    #macd_scan()

    while True:
        if tmr.remain() == 0:
            # Scan and update enabled trading pairs
            pairs = filter_pairs()
            enable_pairs(pairs)
            app.bot.dfc = candles.bulk_load(pairs, TRADEFREQS, dfm=app.bot.dfc)
            # Notify websocket thread to update its sockets
            e_pairs.set()
            macd_scan()
            tmr.reset()
        time.sleep(300)

#------------------------------------------------------------------------------
def filter_pairs():
    scanlog('*'*59)
    try:
        dfT = tickers.binance_24h().sort_values('24hPriceChange')
        dfA = tickers.aggregate_mkt()
    except Exception as e:
        return print("Agg/Ticker Binance client error. {}".format(str(e)))

    filterlist = []
    for pair in dfT.index.tolist():
        q_asset = dfT.loc[pair]['quoteAsset']
        mkt = dfA.loc[q_asset]
        tckr = dfT.loc[pair]

        if all([ fn(tckr, mkt) for fn in TRADE_PAIR_ALGO['filters'] ]):
            filterlist.append(pair)
    return filterlist

#------------------------------------------------------------------------------
def macd_scan():
    for pair in app.bot.get_pairs():
        for freqstr in TRADEFREQS:
            freq = strtofreq(freqstr)
            periods = int((strtoms("now utc") - strtoms(DEF_KLINE_HIST_LEN)) / ((freq * 1000))) #/2))
            df = app.bot.dfc.loc[pair,freq]
            dfh, phases = macd.histo_phases(df, pair, freqstr, periods)

            # Format for log output
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
