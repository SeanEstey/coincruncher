# app.bot.printer
import logging
import tzlocal
import dateparser
from datetime import datetime
import pytz
import pandas as pd
from docs.botconf import trade_pairs as pairs
import app, app.bot
from app.bot import pct_diff
from app.common.utils import colors, to_relative_str, utc_datetime as now
from app.common.timeutils import strtofreq
from . import candles, signals

def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)

log = logging.getLogger('print')

#------------------------------------------------------------------------------
def new_trades(trade_ids):
    db = app.get_db()
    dfc = app.bot.dfc
    cols = ['Freq', "Type", "ΔPrice", "Macd", "RSI", "Time", "Reason"]
    data, indexes = [], []

    for _id in trade_ids:
        record = db.trades.find_one({"_id":_id})
        indexes.append(record['pair'])
        ss1 = record['snapshots'][0]
        ss2 = record['snapshots'][-1]

        if len(record['orders']) > 1:
            c1 = record['orders'][0]['candle']
            c2 = record['orders'][1]['candle']
            df = dfc.loc[record['pair'], strtofreq(c2['freq'])].tail(40)
            data.append([
                c2['freq'],
                'SELL',
                pct_diff(c1['close'], c2['close']),
                ss2['macd']['value'],
                signals.rsi(df['close'], 14),
                to_relative_str(now() - record['start_time']),
                "-"
            ])
        # Buy trade
        else:
            c1 = record['orders'][0]['candle']
            df = dfc.loc[record['pair'], strtofreq(c1['freq'])].tail(40)
            data.append([
                c1['freq'],
                'BUY',
                0.0,
                ss1['macd']['value'],
                signals.rsi(df['close'], 14),
                "-",
                "-"
            ])

    if len(data) == 0:
        return tradelog("0 trades executed")

    df = pd.DataFrame(data, index=pd.Index(indexes), columns=cols)
    df = df[cols]
    lines = df.to_string(formatters={
        cols[0]: ' {}'.format,
        cols[1]: ' {}'.format,
        cols[2]: ' {:+.2f}%'.format,
        cols[3]: ' {:+.3f}'.format,
        cols[4]: '{:.2f}'.format,
        cols[5]: '{}'.format,
        cols[6]: '{}'.format
    }).split("\n")
    tradelog("{} trade(s) executed:".format(len(df)))
    [tradelog(line) for line in lines]

#------------------------------------------------------------------------------
def positions(freqstr):
    """Position summary.
    """
    db = app.get_db()
    dfc = app.bot.dfc
    cols = ["Freq", "ΔPrice", "Macd", "RSI", "Time", "Strategy"]
    data, indexes = [], []
    _trades = list(db.trades.find(
        {'status':'open', 'pair':{"$in":pairs}}))

    for record in _trades:
        c1 = record['orders'][0]['candle']
        c2 = candles.newest(record['pair'], freqstr)
        ss1 = record['snapshots'][0]
        ss2 = record['snapshots'][-1]
        df = dfc.loc[record['pair'], strtofreq(freqstr)].tail(40)

        data.append([
            c1['freq'],
            pct_diff(c1['close'], c2['close']),
            ss2['macd']['value'],
            signals.rsi(df['close'], 14),
            to_relative_str(now() - record['start_time']),
            record['strategy']
        ])
        indexes.append(record['pair'])

    if len(_trades) == 0:
        tradelog("0 open positions")
    else:
        df = pd.DataFrame(data, index=pd.Index(indexes), columns=cols)
        df = df[cols]
        lines = df.to_string(formatters={
            cols[0]: ' {}'.format,
            cols[1]: ' {:+.2f}%'.format,
            cols[2]: '  {:+.3f}'.format,
            cols[3]: '{:.2f}'.format,
            cols[4]: '{}'.format,
            cols[5]: ' {}'.format
        }).split("\n")
        tradelog("{} position(s):".format(len(df)))
        [tradelog(line) for line in lines]
        return df
