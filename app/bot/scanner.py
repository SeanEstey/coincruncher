# app.bot.scanner
import logging
import time
import importlib
import pytz
import pandas as pd
import numpy as np
import docs.botconf
import app, app.bot
from app.common.timer import Timer
from app.common.utils import to_local, utc_datetime as now, strtoms
from app.common.timeutils import strtofreq
from . import enable_pair, update_pairs, get_pairs, macd, tickers, trade
from .candles import api_update, bulk_append_dfc

log = logging.getLogger('scanner')

def scanlog(msg): log.log(98, msg)

#---------------------------------------------------------------------------
def run(e_pairs, e_kill):
    """Main scanner thread loop.
    """
    tmr = Timer(expire='every 15 clock min utc', quiet=True)

    # Initial scan for enabling trading pairs.
    update_pairs([])
    sma_med_trend_filter()

    while True:
        if e_kill.isSet():
            break
        if tmr.remain() == 0:
            # Edit conf w/o having to restart bot.
            importlib.reload(docs.botconf)

            # Reset enabled pairs to only open trades.
            update_pairs([])

            # Scan and enable any additional filtered pairs.
            sma_med_trend_filter()

            tmr.reset()

        time.sleep(3)

    print("Scanner thread: terminating")

#------------------------------------------------------------------------------
def sma_med_trend_filter():
    """Identify pairs in intermediate term uptrend via 1d SMA slope. Enable
    each filtered pair in real-time + load its historic data into memory.
    """
    trend = docs.botconf.TRD_PAIRS['midterm']
    lbl = "sma{}_slope".format(trend['span'])
    freq = strtofreq(trend['freqstr'])

    filtered = trend['filters'][0](tickers.binance_24h())

    results = []
    for pair in filtered:
        bulk_append_dfc(api_update([pair], [trend['freqstr']]))

        sma = app.bot.dfc.loc[pair, freq]['close']\
            .rolling(trend['span']).mean().pct_change()*100

        if all([fn(sma) for fn in trend['conditions']]):
            enable_pair(pair)
            results.append({
                'pair': pair,
                lbl: sma.iloc[-1]
            })

    df = pd.DataFrame(results)\
        .set_index('pair').sort_values(lbl).round(1)

    lines = df.to_string(
        columns=[lbl],
        formatters={lbl:'{:+.1f}%'.format}
    ).split("\n")

    [scanlog(line) for line in lines]
    scanlog("")

    print("Filter scan completed. {} trading pairs enabled. {:,} historic candles loaded."\
        .format(get_pairs(), len(app.bot.dfc)))

    return df

#------------------------------------------------------------------------------
def tckr_med_trend_filter():
    """First pass filter for trading pairs.
    """
    TRD_PAIRS = docs.botconf.TRD_PAIRS

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

        if all([ fn(tckr, mkt) for fn in TRD_PAIRS['filters'] ]):
            filterlist.append(pair)
    return filterlist

#------------------------------------------------------------------------------
def macd_med_trend_filter():
    TRD_FREQS = docs.botconf.TRD_FREQS
    DEF_KLINE_HIST_LEN = docs.botconf.DEF_KLINE_HIST_LEN

    for pair in app.bot.get_pairs():
        for freqstr in TRD_FREQS:
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
