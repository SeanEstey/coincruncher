# app.bnc.printer
import logging
import tzlocal
import dateparser
from datetime import datetime
import pytz
import pandas as pd
import app
from app.common.utils import colors, to_relative_str, utc_datetime as now
import app.bnc
from app.bnc import *
from app.bnc import trade
from .markets import agg_pct_change
def tradelog(msg): log.log(99, msg)
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
def new_trades(trade_ids):
    db = app.get_db()
    dfc = app.bnc.dfc
    cols = ["Type", "ΔPrice", "Slope", "Z-Score", "ΔZ-Score", "Time"]
    data, indexes = [], []

    for _id in trade_ids:
        record = db.trades.find_one({"_id":_id})
        freq_str = record['buy']['candle']['freq']
        indexes.append(record['pair'])
        candle = candles.newest(record['pair'], freq_str, df=dfc)
        ss1 = record['snapshots'][0]
        ss2 = record['snapshots'][-1]

        if record.get('sell'):
            c1 = record['buy']['candle']
            data.append([
                'SELL',
                pct_diff(c1['close'], candle['close']),
                ss2['ema_pct_change'],
                ss2['z_close'],
                ss2['z_close'] - ss1['z_close'],
                to_relative_str(now() - record['start_time'])
            ])
        # Buy trade
        else:
            data.append([
                'BUY',
                0.0,
                ss2['ema_pct_change'],
                ss1['z_close'],
                0.0,
                "-"
            ])

    if len(data) == 0:
        return tradelog("0 trades executed")

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
    tradelog("{} trade(s) executed:".format(len(df)))
    [tradelog(line) for line in lines]

#------------------------------------------------------------------------------
def candle_sig(candle):
    from docs.rules import RULES as rules
    ss = strategy.snapshot(candle)
    color, weight = None, None

    if candle['freq'] == '5m':
        color = colors.GRN
    elif candle['freq'] == '1h' or candle['freq'] == '1d':
        color = colors.BLUE
    else:
        color = colors.WHITE

    threshold = rules['z-score']['buy_thresh']
    if ss['z_close'] < threshold or ss['z_volume'] > threshold*-1:
        color += colors.UNDERLINE

    siglog("{}{:<7} {:>5} {:>+10.2f} z-p {:>+10.2f} z-v {:>10.2f} bv {:>+10.2f} m {:>+10.2f} macd{}{}"\
        .format(color, candle['pair'], candle['freq'], ss['z_close'], ss['z_volume'],
            candle['buy_ratio'], ss['ema_pct_change'], ss['macd_diff'], colors.ENDC, colors.ENDC))

#------------------------------------------------------------------------------
def positions(_type):
    """Position summary.
    @_type: 'open', 'closed'
    @start: datetime.datetime for closed trades
    """
    db = app.get_db()
    dfc = app.bnc.dfc

    if _type == 'open':
        cols = ["ΔPrice", "Slope", " Z-Score", " ΔZ-Score", "Macd", "Time"]
        data, indexes = [], []

        _trades = list(db.trades.find(
            {'status':'open', 'pair':{"$in":pairs}}))

        for record in _trades:
            c1 = record['buy']['candle']
            c2 = candles.newest(record['pair'], c1['freq'], df=dfc)
            ss1 = record['snapshots'][0]
            ss2 = record['snapshots'][-1]

            data.append([
                pct_diff(c1['close'], c2['close']),
                ss2['ema_pct_change'],
                ss2['z_close'],
                ss2['z_close'] - ss1['z_close'],
                ss2['macd_diff'],
                to_relative_str(now() - record['start_time'])
            ])
            indexes.append(record['pair'])

        if len(_trades) == 0:
            tradelog("0 open positions")
        else:
            df = pd.DataFrame(data, index=pd.Index(indexes), columns=cols)
            df = df[cols]
            lines = df.to_string(formatters={
                cols[0]: ' {:+.2f}%'.format,
                cols[1]: ' {:+.2f}%'.format,
                cols[2]: '  {:.2f}'.format,
                cols[3]: '  {:+.2f}'.format,
                cols[4]: '  {:+.2f}'.format,
                cols[5]: '{}'.format
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

        n_win, pct_earn = 0, 0
        for n in closed:
            if n['pct_pdiff'] > 0:
                n_win += 1
            pct_earn += n['pct_earn']

        ratio = (n_win/len(closed))*100 if len(closed) >0 else 0

        tradelog("{} of {} trade(s) today were profitable.".format(n_win, len(closed)))
        duration = to_relative_str(now() - start)
        tradelog("{:+.2f}% net profit today.".format(pct_earn))
