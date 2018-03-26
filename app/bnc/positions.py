
# *********************************************************************
# TODO: Place cap on number of trades to open per cycle and open at
# any given time.
# *********************************************************************

#-------------------------------------------------------------------------------
def eval_open(candle):
    """New trade criteria.
    """
    sig = signals.generate(candle)
    z = sig['z-score']

    # A. Z-Score below threshold
    if z.close < z_thresh:
        return buy(candle, decision={
            'category': 'z-score',
            'signals': sig,
            'details': {
                'close:z-score < thresh': '{:+.2f} < {:+.2f}'.format(
                    z.close, z_thresh)
            }
        })

    # B) Positive market & candle EMA (within Z-Score threshold)
    agg_slope = market.agg_pct_change(dfc, freq_str)
    #agg_slope = mkt_move.iloc[0][0]

    if (sig['ema_slope'].tail(5) > 0).all():
        if agg_slope > 0 and z.volume > 0.5 and z.buy_ratio > 0.5:
            return buy(candle, decision={
                'category': 'slope',
                'signals': sig,
                'details': {
                    'ema:slope:tail(5):min > thresh': '{:+.2f}% > 0'.format(
                        sig['ema_slope'].tail(5).min()),
                    'agg:ema:slope > thresh': '{:+.2f}% > 0'.format(agg_slope),
                    'volume:z-score > thresh': '{+.2f} > 0.5'.format(z.volume),
                    'buy-ratio:z-score > thresh': '{+.2f} > 0.5'.format(z.buy_ratio)
                }
            })

    # No buy executed.
    log.debug("{}{:<5}{:+.2f} Z-Score{:<5}{:+.2f} Slope".format(
        candle['pair'], '', z.close, '', sig['ema_slope'].iloc[-1]))
    return None

#------------------------------------------------------------------------------
def eval_close(doc, candle):
    """Avoid losses, maximize profits.
    """
    sig = signals.generate(candle)
    reason = doc['buy']['decision']['category']

    # A. Predict price peak as we approach mean value.
    if reason == 'z-score':
        if sig['z-score'].close > -0.75:
            if sig['ema_slope'].iloc[-1] <= 0.10:
                return sell(doc, candle, decision={
                    'category': 'z-score',
                    'signals': sig,
                    'details': {
                        'close:z-score > thresh': '{:+.2f} > -0.75'.format(sig['z-score'].close),
                        'ema:slope < thresh': '{:+.2f}% <= 0.10'.format(sig['ema_slope'].iloc[-1])
                    }
                })

    # B. Sell when price slope < 0
    elif reason == 'slope':
        ob = client.get_orderbook_ticker(symbol=candle['pair'])
        bid = float(ob['bidPrice'])

        if sig['ema_slope'].iloc[-1] <= 0 or bid < doc['buy']['order']['price']:
            return sell(doc, candle, orderbook=ob, decision={
                'category': 'slope',
                'signals': sig,
                'details': {
                    'ema:slope <= thresh': '{:+.2f}% <= 0'.format(sig['ema_slope'].iloc[-1]),
                    'OR bid < buy': '{:.8f} < {:.8f}'.format(bid, doc['buy']['order']['price'])
                }
            })
    return None

#------------------------------------------------------------------------------
def open_new(candle, decision=None):
    """Create or update existing position for zscore above threshold value.
    """
    decision['signals']['z-score'] = decision['signals']['z-score'].to_dict()
    decision['signals']['ema_slope'] = sorted(decision['signals']['ema_slope'].to_dict().items())
    orderbook = client.get_orderbook_ticker(symbol=candle['pair'])
    order = {
        'exchange': 'Binance',
        'price': float(orderbook['askPrice']),
        'volume': 1.0,  # FIXME
        'quote': BINANCE['TRADE_AMT'],
        'pct_fee': BINANCE['PCT_FEE'],
        'fee': BINANCE['TRADE_AMT'] * (BINANCE['PCT_FEE']/100),
    }

    return app.get_db().positions.insert_one({
        'pair': candle['pair'],
        'status': 'open',
        'start_time': now(),
        'buy': {
            'time': now(),
            'candle': candle,
            'decision': decision,
            'orderbook': orderbook,
            'order': order
        }
    }).inserted_id

#------------------------------------------------------------------------------
def close_out(doc, candle, orderbook=None, decision=None):
    """Close off existing position and calculate earnings.
    """
    ob = orderbook if orderbook else client.get_orderbook_ticker(symbol=candle['pair'])
    bid = np.float64(ob['bidPrice'])

    pct_fee = BINANCE['PCT_FEE']
    buy_vol = np.float64(doc['buy']['order']['volume'])
    buy_quote = np.float64(doc['buy']['order']['quote'])
    p1 = np.float64(doc['buy']['order']['price'])

    pct_pdiff = pct_diff(p1, bid)
    quote = (bid * buy_vol) * (1 - pct_fee/100)
    fee = (bid * buy_vol) * (pct_fee/100)

    net_earn = quote - buy_quote
    pct_net = pct_diff(buy_quote, quote)

    duration = now() - doc['start_time']
    candle['buy_ratio'] = candle['buy_ratio'].round(4)

    decision['signals']['z-score'] = decision['signals']['z-score'].to_dict()
    decision['signals']['ema_slope'] = sorted(decision['signals']['ema_slope'].to_dict().items())

    app.get_db().positions.update_one(
        {'_id': doc['_id']},
        {'$set': {
            'status': 'closed',
            'end_time': now(),
            'duration': int(duration.total_seconds()),
            'pct_pdiff': pct_pdiff.round(4),
            'pct_earn': pct_net.round(4),
            'net_earn': net_earn.round(4),
            'sell': {
                'time': now(),
                'candle': candle,
                'decision': decision,
                'orderbook': ob,
                'order': {
                    'exchange':'Binance',
                    'price': bid,
                    'volume': 1.0,
                    'quote': quote,
                    'pct_fee': pct_fee,
                    'fee': fee
                }
            }
        }}
    )
    return doc['_id']

#------------------------------------------------------------------------------
def open_summary():
    cols = ["ΔPrice", "Slope", " Z-Score", " ΔZ-Score", "Time"]
    data, indexes = [], []
    holdings = list(app.get_db().positions.find({'status':'open', 'pair':{"$in":pairs}}))

    for doc in holdings:
        c1 = doc['buy']['candle']
        c2 = candles.newest(doc['pair'], freq_str, df=dfc)
        sig = signals.generate(c2)

        data.append([
            pct_diff(c1['close'], c2['close']),
            sig['ema_slope'].iloc[-1],
            sig['z-score'].close,
            sig['z-score'].close - doc['buy']['decision']['signals']['z-score']['close'],
            to_relative_str(now() - doc['start_time'])
        ])
        indexes.append(doc['pair'])

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
def closed_summary(t1):
    """
    """
    db = app.get_db()
    n_win, pct_earn = 0, 0
    closed = list(db.positions.find({"status":"closed"}))

    for n in closed:
        if n['pct_pdiff'] > 0:
            n_win += 1
        pct_earn += n['pct_earn']

    ratio = (n_win/len(closed))*100 if len(closed) >0 else 0

    #siglog("Cycle time: {:,.0f} ms".format(t1.elapsed()))
    siglog("{} of {} trade(s) today were profitable.".format(n_win, len(closed)))
    duration = to_relative_str(now() - start)
    siglog("{:+.2f}% net profit today.".format(pct_earn))
