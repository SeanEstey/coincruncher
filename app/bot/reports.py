# app.bot.printer
import logging
from datetime import datetime
from pprint import pformat
import pandas as pd
from docs.botconf import *
from docs.conf import *
import app, app.bot
from app.common.utils import pct_diff, to_relative_str, utc_datetime as now
from app.common.timeutils import strtofreq
from . import macd, signals

def tradelog(msg): log.log(99, msg)
log = logging.getLogger('reports')

#------------------------------------------------------------------------------
def trades(trade_ids):
    db = app.get_db()
    cols = ['freq', "type", "Δprice", "macd", "rsi", "zscore", "time", "algo", "details"]
    data, indexes = [], []

    for _id in trade_ids:
        record = db.trades.find_one({"_id":_id})
        indexes.append(record['pair'])
        ss1 = record['snapshots'][0]
        ss_new = record['snapshots'][-1]
        df = app.bot.dfc.loc[record['pair'], strtofreq(record['freqstr'])].tail(100)

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
                record['algo'],
                record['details'][-1]['section'].title()
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
                record['algo'],
                record['details'][-1]['section'].title()
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
        cols[5]: '{}'.format,
        cols[6]: '{}'.format
    }).split("\n")

    tradelog('-'*TRADELOG_WIDTH)
    tradelog("{} trade(s) executed:".format(len(df)))
    [tradelog(line) for line in lines]

#------------------------------------------------------------------------------
def positions():
    """Position summary.
    """
    db = app.get_db()
    cols = ["freq", "price", "Δprice", "macd", "rsi", "zscore", "time", "algo"]
    data, indexes = [], []
    opentrades = db.trades.find({'status':'open'})

    for record in opentrades:
        ss1 = record['snapshots'][0]
        c1 = ss1['candle']
        ss_new = record['snapshots'][-1]
        freq = strtofreq(record['freqstr'])
        df = app.bot.dfc.loc[record['pair'], freq]
        dfmacd, phases = macd.histo_phases(df, record['pair'], record['freqstr'], 100)

        data.append([
            c1['freqstr'],
            df.iloc[-1]['close'],
            pct_diff(c1['close'], df.iloc[-1]['close']),
            phases[-1].iloc[-1],
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
            cols[1]: ' {:g}'.format,
            cols[2]: ' {:+.2f}%'.format,
            cols[3]: '  {:+.3f}'.format,
            cols[4]: '{:.0f}'.format,
            cols[5]: '{}'.format,
            cols[6]: ' {}'.format
        }).split("\n")
        tradelog('-'*TRADELOG_WIDTH)
        tradelog("{} position(s):".format(len(df)))
        [tradelog(line) for line in lines]
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

    tradelog('-'*TRADELOG_WIDTH)
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
