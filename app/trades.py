# *********************************************************************
# TODO: Place cap on number of trades to open per cycle and open at
# any given time.
# *********************************************************************
import logging
from datetime import timedelta as delta
import pandas as pd
import numpy as np
from pymongo import UpdateOne, ReturnDocument
from pprint import pprint
import app
from app import freqtostr, strtofreq, pertostr, candles, signals
from app.utils import utc_datetime as now, to_relative_str
from app.timer import Timer
from docs.rules import RULES
from docs.data import BINANCE

def siglog(msg): log.log(100, msg)
def pct_diff(a,b): return ((b-a)/a)*100
log = logging.getLogger('trades')

rules = RULES['1m'] # FIXME
dfc = pd.DataFrame()
n_cycles = 0
mkt_move, freq, freq_str = None, None, None
pairs = BINANCE['PAIRS']
ema_span = rules['EMA']['SPAN']
z_thresh = rules['Z-SCORE']['THRESH']


#------------------------------------------------------------------------------
def init():
    """Preload candles records from mongoDB to global dataframe.
    Performance: ~3,000ms/100k records
    """
    global dfc
    t1 = Timer()
    log.info('Preloading historic data...')

    dfc = pd.DataFrame()
    dfc = candles.merge(dfc, pairs, time_span=delta(days=21))

    log.info('{:,} records loaded in {:,.1f}s.'.format(
        len(dfc), t1.elapsed(unit='s')))

#------------------------------------------------------------------------------
def update(_freq_str):
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
    mkt_move = signals.pct_mkt_change(dfc, freq_str)

    siglog('*'*80)
    siglog("Cycle #{}".format(n_cycles))
    siglog('*'*80)
    siglog("{} trading pair(s):".format(len(pairs)))
    [siglog(x) for x in agg_mkt().to_string().split('\n')]
    siglog('-'*80)
    hold_summary()
    siglog('-'*80)

    trade_ids=[]

    # Evaluate Sells
    holdings = list(db.trades.find({'status':'open', 'pair':{"$in":pairs}}))
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
def calc_signals(candle):
    df = dfc.loc[candle['pair'],freq]
    rng = df.loc[slice(
        candle['open_time'] - delta(hours=2),
        candle['open_time']
    )]['close'].copy()
    ema_slope = rng.ewm(span=ema_span).mean().pct_change().tail(10) * 100
    ema_slope.index = [ str(n)[:-10] for n in ema_slope.index.values ]
    ema_slope = ema_slope.round(3)

    return {
        'z_score': signals.z_score(df, candle),
        'ema_slope': ema_slope
    }

#-------------------------------------------------------------------------------
def eval_buy(candle):
    """New trade criteria.
    """
    cc,cp = candle['close'], candle['pair']
    sig = calc_signals(candle)
    z = sig['z_score']

    log.debug("{} {:+.2f} ZP, {:.8g} P".format(cp.lower(), z.close, cc))

    # A. Z-Score below threshold
    if z.close < z_thresh:
        return buy(candle, sig, 'zthresh', extra="{:+.2f} ZP < {:.2f}.".format(
            z.close, z_thresh))

    # B) Positive market & candle EMA (within Z-Score threshold)
    mslope = mkt_move.iloc[0][0]

    if (sig['ema_slope'].tail(5) > 0).all():
        if mslope > 0 and z.volume > 0.5 and z.buy_ratio > 0.5:
            details = '{:+.8g} mslope, {:+.5f}% pslope, {:+.2f} z.close.'.format(
                mslope, sig['ema_slope'].iloc[-1], z.close)
            return buy(candle, sig, 'pslope', extra=details)
    return None

#------------------------------------------------------------------------------
def eval_sell(hold, candle):
    """Avoid losses, maximize profits.
    """
    sig = calc_signals(candle)
    reason = hold['buy'].get('reason','')

    # A. Predict price peak as we approach mean value.
    if reason == 'zthresh':
        if sig['z_score'].close > -0.75:
            return sell(hold, candle, sig, 'zthresh')
    # B. Sell when price slope < 0
    elif reason == 'pslope':
        if sig['ema_slope'].iloc[-1] <= 0 or (candle['close'] - hold['buy']['price']) < 0:
            return sell(hold, candle, sig, 'pslope')
    return None

#------------------------------------------------------------------------------
def buy(candle, sig, reason, extra=None):
    """Create or update existing position for zscore above threshold value.
    """
    pct_fee = BINANCE['PCT_FEE']
    quote = BINANCE['TRADE_AMT']
    fee = quote * (pct_fee/100)

    sig['z_score'] = sig['z_score'].to_dict()
    sig['ema_slope'] = sig['ema_slope'].to_dict()

    return app.get_db().trades.insert_one({
        'pair': candle['pair'],
        'status': 'open',
        'start_time': now(),
        'exchange': 'Binance',
        'buy': {
            'time': now(),
            'reason': reason,
            'price': candle['close'].round(8),
            'volume': np.float64(quote / candle['close']).round(8),
            'quote': quote + fee,
            'fee': fee,
            'pct_fee': pct_fee,
            'candle': candle,
            'signals': sig,
            'details': extra if extra else None
        }
    }).inserted_id

#------------------------------------------------------------------------------
def sell(hold, candle, sig, reason):
    """Close off existing position and calculate earnings.
    """
    pct_fee = BINANCE['PCT_FEE']
    buy_vol = np.float64(hold['buy']['volume'])
    buy_quote = np.float64(hold['buy']['quote'])
    p1 = np.float64(hold['buy']['candle']['close'])

    p2 = candle['close']
    pct_pdiff = pct_diff(p1, p2)
    quote = (p2 * buy_vol) * (1 - pct_fee/100)
    fee = (p2 * buy_vol) * (pct_fee/100)

    net_earn = quote - buy_quote
    pct_net = pct_diff(buy_quote, quote)

    duration = now() - hold['start_time']
    candle['buy_ratio'] = candle['buy_ratio'].round(4)

    sig['z_score'] = sig['z_score'].to_dict()
    sig['ema_slope'] = sig['ema_slope'].to_dict()

    db = app.get_db()
    db.trades.update_one(
        {'_id': hold['_id']},
        {'$set': {
            'status': 'closed',
            'end_time': now(),
            'duration': int(duration.total_seconds()),
            'pct_pdiff': pct_pdiff.round(4),
            'pct_earn': pct_net.round(4),
            'net_earn': net_earn.round(4),
            'sell': {
                'time': now(),
                'reason': reason,
                'price': p2.round(8),
                'volume': buy_vol.round(8),
                'quote': quote.round(8),
                'fee': fee,
                'pct_fee': pct_fee,
                'candle': candle,
                'signals': sig
            }
        }}
    )
    return hold['_id']

#------------------------------------------------------------------------------
def hold_summary():
    cols = ["ΔPrice", "Slope", " Z-Score", " ΔZ-Score", "Time"]
    data, indexes = [], []
    holdings = list(app.get_db().trades.find({'status':'open', 'pair':{"$in":pairs}}))

    for hold in holdings:
        c1 = hold['buy']['candle']
        c2 = candles.newest(hold['pair'], freq_str, df=dfc)
        sig = calc_signals(c2)

        data.append([
            pct_diff(c1['close'], c2['close']),
            sig['ema_slope'].iloc[-1],
            sig['z_score'].close,
            sig['z_score'].close - hold['buy']['signals']['z_score']['close'],
            to_relative_str(now() - hold['start_time'])
        ])
        indexes.append(hold['pair'])

    if len(holdings) == 0:
        siglog(" 0 holdings")
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
        siglog("{} holding(s):".format(len(df)))
        [siglog(line) for line in lines]
        return df

#------------------------------------------------------------------------------
def cycle_summary(trade_ids):
    db = app.get_db()
    cols = ["Type", "ΔPrice", "Slope", "Z-Score", "ΔZ-Score", "Time"]
    data, indexes = [], []

    for _id in trade_ids:
        hold = db.trades.find_one({"_id":_id})
        indexes.append(hold['pair'])
        candle = candles.newest(hold['pair'], freq_str, df=dfc)
        sig = calc_signals(candle)

        if hold.get('sell'):
            c1 = hold['buy']['candle']
            z1 = hold['buy']['signals']['z_score']
            data.append([
                'SELL',
                pct_diff(c1['close'], candle['close']),
                sig['ema_slope'].iloc[-1],
                sig['z_score'].close,
                sig['z_score'].close - z1['close'],
                to_relative_str(now() - hold['start_time'])
            ])
        # Buy trade
        else:
            # TODO: include buy_ratio / volume?
            data.append([
                'BUY',
                0.0,
                sig['ema_slope'].iloc[-1],
                hold['buy']['signals']['z_score']['close'],
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
def total_summary(t1):
    """
    """
    db = app.get_db()
    n_win, pct_earn = 0, 0
    closed = list(db.trades.find({"status":"closed"}))

    for n in closed:
        if n['pct_pdiff'] > 0:
            n_win += 1
        pct_earn += n['pct_earn']

    ratio = (n_win/len(closed))*100 if len(closed) >0 else 0

    #siglog("Cycle time: {:,.0f} ms".format(t1.elapsed()))
    siglog("{} of {} trade(s) profited".format(n_win, len(closed)))  #, ratio))
    siglog("{:+.2f}% net earnings".format(pct_earn))

#-----------------------------------------------------------------------------
def agg_mkt():
    """
    """
    labels = ['5 min', '1 hr', '4 hrs', '12 hrs', '24 hrs']
    row_label = 'Agg.Price'

    _list = [
        signals.pct_mkt_change(dfc, '1m', span=5, label='5 min'),
        signals.pct_mkt_change(dfc, '1m', span=60, label='1 hr'),
        signals.pct_mkt_change(dfc, '1h', span=4, label='4 hr'),
        signals.pct_mkt_change(dfc, '1h', span=12, label='12 hr'),
        signals.pct_mkt_change(dfc, '1h', span=24, label='24 hr')
    ]

    df = pd.DataFrame(
        {labels[n]:_list[n] for n in range(0,len(labels))},
        index=[row_label])
    df = df[labels]

    # Print values to % str
    for n in range(0,len(labels)):
        value = df[df.columns[n]][0]
        df.set_value(row_label, df.columns[n], "{:+,.2f}%".format(value))
    return df

#-----------------------------------------------------------------------------
def print_tickers():
    # *********************************************************************
    # TODO: Create another trading log for detailed ticker tarding signals.
    # Primary siglog will be mostly for active trading/holdings.
    # *********************************************************************
    pass
