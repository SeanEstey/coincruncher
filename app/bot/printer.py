# app.bot.printer
import logging
import tzlocal
import dateparser
from datetime import datetime
import pytz
import pandas as pd
from docs.conf import trade_pairs as pairs
import app, app.bot
from app.bot import pct_diff
from app.common.utils import colors, to_relative_str, utc_datetime as now
from . import candles

def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)

log = logging.getLogger('print')

#------------------------------------------------------------------------------
def new_trades(trade_ids):
    db = app.get_db()
    dfc = app.bot.dfc
    cols = ['Freq', "Type", "ΔPrice", "Z-Score", "ΔZ-Score", "Macd", "Time"]
    data, indexes = [], []

    for _id in trade_ids:
        record = db.trades.find_one({"_id":_id})
        freq_str = record['orders'][0]['candle']['freq']
        indexes.append(record['pair'])
        candle = candles.newest(record['pair'], freq_str)
        ss1 = record['snapshots'][0]
        ss2 = record['snapshots'][-1]

        if len(record['orders']) > 1:
            c1 = record['orders'][0]['candle']
            data.append([
                candle['freq'],
                'SELL',
                pct_diff(c1['close'], candle['close']),
                ss2['price']['z-score'],
                ss2['price']['z-score'] - ss1['price']['z-score'],
                ss2['macd']['value'],
                to_relative_str(now() - record['start_time'])
            ])
        # Buy trade
        else:
            data.append([
                candle['freq'],
                'BUY',
                0.0,
                ss1['price']['z-score'],
                0.0,
                ss1['macd']['value'],
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
        cols[3]: ' {:+.2f}'.format,
        cols[4]: ' {:+.2f}'.format,
        cols[5]: ' {:+.3f}'.format,
        cols[6]: '{}'.format
    }).split("\n")
    tradelog("{} trade(s) executed:".format(len(df)))
    [tradelog(line) for line in lines]

#------------------------------------------------------------------------------
def positions(_type):
    """Position summary.
    @_type: 'open', 'closed'
    @start: datetime.datetime for closed trades
    """
    db = app.get_db()
    dfc = app.bot.dfc

    if _type == 'open':
        cols = ["Freq", "ΔPrice", " Z-Score", " ΔZ-Score", "Macd", "Time", "Strategy"]
        data, indexes = [], []

        _trades = list(db.trades.find(
            {'status':'open', 'pair':{"$in":pairs}}))

        for record in _trades:
            c1 = record['orders'][0]['candle']
            c2 = candles.newest(record['pair'], c1['freq'])
            ss1 = record['snapshots'][0]
            ss2 = record['snapshots'][-1]

            data.append([
                c1['freq'],
                pct_diff(c1['close'], c2['close']),
                ss2['price']['z-score'],
                ss2['price']['z-score'] - ss1['price']['z-score'],
                ss2['macd']['value'],
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
                cols[2]: '  {:.2f}'.format,
                cols[3]: '  {:+.2f}'.format,
                cols[4]: '  {:+.3f}'.format,
                cols[5]: '{}'.format,
                cols[6]: ' {}'.format
            }).split("\n")
            tradelog("{} position(s):".format(len(df)))
            [tradelog(line) for line in lines]
            return df
    elif _type == 'closed':
        if datetime.now().time().hour >= 8:
            start = dateparser.parse("8 am today").replace(
                tzinfo=tzlocal.get_localzone()).astimezone(pytz.utc)
        else:
            start = dateparser.parse("8 am yesterday").replace(
                tzinfo=tzlocal.get_localzone()).astimezone(pytz.utc)

        closed = list(db.trades.find(
            {'status':'closed', 'end_time':{'$gte':start}}))

        #print("%s trades today ending after %s" % (len(closed), start))

        n_win, pct_net_gain = 0, 0
        for n in closed:
            if n['pct_net_gain'] > 0:
                n_win += 1
            pct_net_gain += n['pct_net_gain']

        ratio = (n_win/len(closed))*100 if len(closed) >0 else 0

        tradelog("{} of {} trade(s) today were profitable.".format(n_win, len(closed)))
        duration = to_relative_str(now() - start)
        tradelog("{:+.2f}% net profit today.".format(pct_net_gain))
