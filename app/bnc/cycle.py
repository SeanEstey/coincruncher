import logging
from datetime import timedelta as delta
import pandas as pd
import numpy as np
from binance.client import Client
from pymongo import UpdateOne
import app
from app import strtofreq
from app.utils import utc_datetime as now, to_relative_str
from . import candles, signals
from app.timer import Timer

log = logging.getLogger('trades')

#------------------------------------------------------------------------------
def init():
    """Preload candles records from mongoDB to global dataframe.
    Performance: ~3,000ms/100k records
    """
    global dfc, client
    t1 = Timer()
    log.info('Preloading historic data...')

    dfc = pd.DataFrame()
    dfc = candles.merge(dfc, pairs, time_span=delta(days=21))

    client = Client("", "")

    log.info('{:,} records loaded in {:,.1f}s.'.format(
        len(dfc), t1.elapsed(unit='s')))

#------------------------------------------------------------------------------
def update(frequency):
    """Evaluate Binance market data and execute buy/sell trades.
    """
    global dfc, n_cycles, freq_str, freq, mkt_move
    freq_str = _freq_str
    freq = strtofreq[freq_str]
    t1 = Timer()
    db = app.get_db()

    # Update candle data
    dfc = candles.merge(dfc, pairs, time_span=delta(minutes=10))
    # Bullish/Bearish market movement
    #mkt_move = market.pct_change(dfc, freq_str)

    siglog('*'*80)
    duration = to_relative_str(now() - start)
    hdr = "Cycle #{} {:>%s}" % (80 - 7 - 1 - len(str(n_cycles)))
    siglog(hdr.format(n_cycles, duration))
    siglog('*'*80)
    siglog("{} trading pair(s):".format(len(pairs)))
    [siglog(x) for x in agg_mkt().to_string().split('\n')]
    siglog('-'*80)
    hold_summary()
    siglog('-'*80)

    trade_ids=[]

    # Evaluate Sells
    holdings = list(db.positions.find({'status':'open', 'pair':{"$in":pairs}}))
    for hold in holdings:
        candle = candles.newest(hold['pair'], freq_str, df=dfc)
        trade_ids.append(eval_sell(hold, candle))

    # Evaluate Buys
    inactive = sorted(list(set(pairs) - set([n['pair'] for n in holdings])))
    for pair in inactive:
        candle = candles.newest(pair, freq_str, df=dfc)
        trade_ids.append(eval_buy(candle))

    cycle_summary([n for n in trade_ids if n])
    siglog('-'*80)
    total_summary(t1)
    n_cycles +=1

#------------------------------------------------------------------------------
def summary(trade_ids):
    db = app.get_db()
    cols = ["Type", "ΔPrice", "Slope", "Z-Score", "ΔZ-Score", "Time"]
    data, indexes = [], []

    for _id in trade_ids:
        doc = db.positions.find_one({"_id":_id})
        indexes.append(doc['pair'])
        candle = candles.newest(doc['pair'], freq_str, df=dfc)
        sig = signals.generate(candle)

        if doc.get('sell'):
            c1 = doc['buy']['candle']
            z1 = doc['buy']['decision']['signals']['z-score']
            data.append([
                'SELL',
                pct_diff(c1['close'], candle['close']),
                sig['ema_slope'].iloc[-1],
                sig['z-score'].close,
                sig['z-score'].close - z1['close'],
                to_relative_str(now() - doc['start_time'])
            ])
        # Buy trade
        else:
            data.append([
                'BUY',
                0.0,
                sig['ema_slope'].iloc[-1],
                doc['buy']['decision']['signals']['z-score']['close'],
                0.0,
                "-"
            ])

    if len(data) == 0:
        return siglog("0 trades executed")

    df = pd.DataFrame(data, index=pd.Index(indexes), columns=cols)
    df = df[cols]
    lines = df.to_string(formatters={
        cols[0]: ' {}'.format,
        cols[1]: ' {:+.2f}%'.format,
        cols[2]: ' {:+.2f}'.format,
        cols[3]: ' {:+.2f}'.format,
        cols[4]: ' {:+.2f}'.format,
        cols[5]: '{}'.format
    }).split("\n")
    siglog("{} trade(s) executed:".format(len(df)))
    [siglog(line) for line in lines]

