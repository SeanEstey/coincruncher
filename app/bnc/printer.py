# app.bnc.printer
import logging
import pandas as pd
import app
from app.common.utils import to_relative_str, utc_datetime as now
import app.bnc
from app.bnc import *
from .markets import agg_pct_change
def siglog(msg): log.log(100, msg)
log = logging.getLogger('print')

#-----------------------------------------------------------------------------
def agg_mkts():
    """
    """
    dfc = app.bnc.dfc
    labels = ['5 min', '1 hr', '4 hrs', '12 hrs', '24 hrs']
    row_label = 'Agg.Price'
    _list = [
        agg_pct_change('1m', span=5, label='5 min'),
        agg_pct_change('1m', span=60, label='1 hr'),
        agg_pct_change('1h', span=4, label='4 hr'),
        agg_pct_change('1h', span=12, label='12 hr'),
        agg_pct_change('1h', span=24, label='24 hr')
    ]
    df = pd.DataFrame(
        {labels[n]:_list[n] for n in range(0,len(labels))},
        index=[row_label])
    df = df[labels]

    # Print values to % str
    for n in range(0,len(labels)):
        value = df[df.columns[n]][0]
        # FIXME. Deprecated set_value()
        df.set_value(row_label, df.columns[n], "{:+,.2f}%".format(value))
    return df

#------------------------------------------------------------------------------
def trades(trade_ids):
    db = app.get_db()
    dfc = app.bnc.dfc
    cols = ["Type", "ΔPrice", "Slope", "Z-Score", "ΔZ-Score", "Time"]
    data, indexes = [], []

    for _id in trade_ids:
        doc = db.trades.find_one({"_id":_id})
        freq_str = doc['buy']['candle']['freq']
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

#------------------------------------------------------------------------------
def positions(_type, start=None):
    db = app.get_db()
    dfc = app.bnc.dfc

    if _type == 'open':
        cols = ["ΔPrice", "Slope", " Z-Score", " ΔZ-Score", "Time"]
        data, indexes = [], []
        trades = list(db.trades.find({'status':'open', 'pair':{"$in":pairs}}))

        for doc in trades:
            c1 = doc['buy']['candle']
            c2 = candles.newest(doc['pair'], c1['freq'], df=dfc)
            sig = signals.generate(c2)

            data.append([
                pct_diff(c1['close'], c2['close']),
                sig['ema_slope'].iloc[-1],
                sig['z-score'].close,
                sig['z-score'].close - doc['buy']['decision']['signals']['z-score']['close'],
                to_relative_str(now() - doc['start_time'])
            ])
            indexes.append(doc['pair'])

        if len(trades) == 0:
            siglog(" 0 open positions")
        else:
            df = pd.DataFrame(data, index=pd.Index(indexes), columns=cols)
            df = df[cols]
            lines = df.to_string(formatters={
                cols[0]: ' {:+.2f}%'.format,
                cols[1]: ' {:+.2f}%'.format,
                cols[2]: '  {:.2f}'.format,
                cols[3]: '  {:+.2f}'.format,
                cols[4]: '{}'.format
            }).split("\n")
            siglog("{} position(s):".format(len(df)))
            [siglog(line) for line in lines]
            return df
    elif _type == 'closed':
        n_win, pct_earn = 0, 0
        closed = list(db.trades.find({"status":"closed"}))

        for n in closed:
            if n['pct_pdiff'] > 0:
                n_win += 1
            pct_earn += n['pct_earn']

        ratio = (n_win/len(closed))*100 if len(closed) >0 else 0

        siglog("{} of {} trade(s) today were profitable.".format(n_win, len(closed)))
        duration = to_relative_str(now() - start)
        siglog("{:+.2f}% net profit today.".format(pct_earn))
