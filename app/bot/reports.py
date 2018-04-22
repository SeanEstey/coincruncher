# app.bot.printer
import logging
import tzlocal
import dateparser
from datetime import datetime
import pytz
import pandas as pd
from docs.botconf import *
from docs.conf import *
import app, app.bot
from app.common.utils import colors, pct_diff, to_relative_str, utc_datetime as now
from app.common.timeutils import strtofreq
from . import candles, macd, signals

def tradelog(msg): log.log(99, msg)
def siglog(msg): log.log(100, msg)
log = logging.getLogger('reports')

#------------------------------------------------------------------------------
def trades(trade_ids):
    db = app.get_db()
    dfc = app.bot.dfc
    cols = ['freq', "type", "Δprice", "macd", "rsi", "zscore", "time", "details"]
    data, indexes = [], []

    for _id in trade_ids:
        record = db.trades.find_one({"_id":_id})
        indexes.append(record['pair'])
        ss1 = record['snapshots'][0]
        ss_new = record['snapshots'][-1]
        df = dfc.loc[record['pair'], strtofreq(record['freqstr'])].tail(100)

        if len(record['orders']) > 1:
            c1 = ss1['candle']
            c2 = ss_new['candle']
            data.append([
                c2['freqstr'],
                'SELL',
                pct_diff(c1['close'], c2['close']),
                ss_new['indicators']['macd']['value'],
                ss_new['indicators']['rsi'],
                ss_new['indicators']['zscore'],
                to_relative_str(now() - record['start_time']),
                record['details']
            ])
        # Buy trade
        else:
            c1 = ss1['candle']
            data.append([
                c1['freqstr'],
                'BUY',
                0.0,
                ss_new['indicators']['macd']['value'],
                ss_new['indicators']['rsi'],
                ss_new['indicators']['zscore'],
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
        cols[4]: '{:.0f}'.format,
        cols[5]: '{}'.format,
        cols[6]: '{}'.format
    }).split("\n")

    tradelog('-'*TRADELOG_WIDTH)
    tradelog("{} trade(s) executed:".format(len(df)))
    [tradelog(line) for line in lines]
    tradelog('-'*TRADELOG_WIDTH)

#------------------------------------------------------------------------------
def positions():
    """Position summary.
    """
    db = app.get_db()
    dfc = app.bot.dfc
    cols = ["freq", "Δprice", "macd", "rsi", "zscore", "time", "algo"]
    data, indexes = [], []
    opentrades = db.trades.find({'status':'open'})

    for record in opentrades:
        ss1 = record['snapshots'][0]
        c1 = ss1['candle']
        ss_new = record['snapshots'][-1]
        df = dfc.loc[record['pair'], strtofreq(record['freqstr'])].tail(100).copy()
        freq = strtofreq(record['freqstr'])

        data.append([
            c1['freqstr'],
            pct_diff(c1['close'], df.iloc[-1]['close']),
            macd.generate(df)['macd_diff'].iloc[-1],
            signals.rsi(df['close'], 14),
            signals.zscore(df['close'], df.iloc[-1]['close'], 21),
            to_relative_str(now() - record['start_time']),
            record['algo']
        ])
        indexes.append(record['pair'])

    if opentrades.count() == 0:
        tradelog("0 open positions")
    else:
        df = pd.DataFrame(data, index=pd.Index(indexes), columns=cols)
        df = df[cols]
        lines = df.to_string(formatters={
            cols[0]: ' {}'.format,
            cols[1]: ' {:+.2f}%'.format,
            cols[2]: '  {:+.3f}'.format,
            cols[3]: '{:.0f}'.format,
            cols[4]: '{}'.format,
            cols[5]: ' {}'.format
        }).split("\n")
        tradelog("{} position(s):".format(len(df)))
        [tradelog(line) for line in lines]
        tradelog('-'*TRADELOG_WIDTH)
        return df

#-------------------------------------------------------------------------------
def earnings():
    """Performance summary of trades, grouped by day/strategy.
    """
    db = app.get_db()

    gain = list(db.trades.aggregate([
        {'$match': {'status':'closed', 'pct_net_gain':{'$gte':0}}},
        {'$group': {
            '_id': {'algo':'$algo', 'day': {'$dayOfYear':'$end_time'}},
            'total': {'$sum':'$pct_net_gain'},
            'count': {'$sum': 1}
        }}
    ]))
    loss = list(db.trades.aggregate([
        {'$match': {'status':'closed', 'pct_net_gain':{'$lt':0}}},
        {'$group': {
            '_id': {'algo':'$algo', 'day': {'$dayOfYear':'$end_time'}},
            'total': {'$sum':'$pct_net_gain'},
            'count': {'$sum': 1}
        }}
    ]))
    assets = list(db.trades.aggregate([
        { '$match': {'status':'closed', 'pct_net_gain':{'$gte':0}}},
        { '$group': {
            '_id': {
                'asset':'$quote_asset',
                'day': {'$dayOfYear':'$end_time'}},
            'total': {'$sum':'$pct_net_gain'},
            'count': {'$sum': 1}
        }}
    ]))

    today = int(datetime.utcnow().strftime('%j'))
    gain = [ n for n in gain if n['_id']['day'] == today]
    loss = [ n for n in loss if n['_id']['day'] == today]

    for n in gain:
        tradelog("{:} today: {:} wins ({:+.2f}%)."\
            .format(
                n['_id']['algo'],
                n['count'],
                n['total']
            ))
    for n in loss:
        tradelog("{:} today: {:} losses ({:+.2f}%)."\
            .format(
                n['_id']['algo'],
                n['count'],
                n['total']
            ))
    return (gain, loss, assets)
