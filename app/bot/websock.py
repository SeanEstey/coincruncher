""" app.bot.websock
python-binance docs:
http://python-binance.readthedocs.io/en/latest/overview.html
Binance wss docs:
https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md
"""
import logging
import itertools
import time
import sys
import pandas as pd
import numpy as np
from twisted.internet import reactor
from binance.websockets import BinanceSocketManager
from docs.botconf import *
import app, app.bot
from app.common.utils import colors as c
from app.common.timer import Timer
from app.bot import get_pairs
from main import q_open, q_closed

spinner = itertools.cycle(['-', '/', '|', '\\'])
connkeys, storedata = [], []
ws = None

#---------------------------------------------------------------------------
def run(e_pairs):
    global storedata, connkeys

    tmr = Timer(name='pairs', expire='every 5 clock min utc')
    print("Connecting to websocket...")
    client = app.bot.client
    ws = BinanceSocketManager(client)
    pairs = get_pairs()

    connkeys += [ws.start_kline_socket(pair, recv_kline, interval=n) \
        for n in TRADEFREQS for pair in pairs]
    print("Subscribed to {} kline sockets.".format(len(connkeys)))
    ws.start()
    print('Connected. Press Ctrl+C to quit')

    pairset = set([n[0:n.index('@')].upper() for n in connkeys])

    while True:
        if e_pairs.isSet():
            update_sockets()
            e_pairs.clear()

        if tmr.remain(quiet=True) == 0:
            tmr.reset(quiet=True)
            if len(storedata) > 0:
                print("SAVING CANDLES TO DB")
                storedata = []

        update_spinner()
        time.sleep(0.1)

    close_all()

#---------------------------------------------------------------------------
def update_sockets():
    """conn_key str format: <symbol>@kline_<interval>
    """
    global connkeys
    print("Pair event triggered.")
    old = set([n[0:n.index('@')].upper() for n in connkeys])
    new = set(app.bot.get_pairs())

    # Removed pairs: close all sockets w/ matching symbols.
    for pair in (old - new):
        for k in connkeys:
            if k.index(pair) > -1:
                ws.stop_socket(k)
                idx = connkeys.index(k)
                connkeys = connkeys[0:idx] + connkeys[idx+1:]
    print("{} pair sockets removed.".format(len(old-new)))

    # Added pairs: create sockets for each candle frequency.
    newpairs = new - old
    connkeys += [ws.start_kline_socket(i, recv_kline, interval=j) for j in TRADEFREQS for i in newpairs]
    print("{} pair sockets created.".format(len(newpairs)))

#---------------------------------------------------------------------------
def recv_kline(msg):
    global storedata

    if msg['e'] != 'kline':
        print('not a kline: {}'.format(msg))
        return

    k = msg['k']
    candle = {
        "open_time": pd.to_datetime(k['t'], unit='ms', utc=True),
        "close_time": pd.to_datetime(k['T'], unit='ms', utc=True),
        "pair": k['s'],
        "freqstr": k['i'],
        "open": np.float64(k['o']),
        "close": np.float64(k['c']),
        "high": np.float64(k['h']),
        "low": np.float64(k['l']),
        "trades": k['n'],
        "volume": np.float64(k['v']),
        "buy_vol": np.float64(k['V']),
        "quote_volume": np.float64(k['q']),
        "quote_buy_vol": np.float64(k['Q'])
    }

    if k['x'] == True:
        q_closed.put(candle)
        storedata.append(candle)

        print("{}{:<7}{}{:>5}{:>12g}{}".format(c.GRN, candle['pair'], c.WHITE,
            candle['freqstr'], candle['close'], c.ENDC))
    else:
        q_open.put(candle)

#---------------------------------------------------------------------------
def save_klines():
    """Bulk write all new kline data to mongodb. Should be callled on a
    timer every ~60sec.
    """
    pass

#---------------------------------------------------------------------------
def close_all():
    print('Closing all sockets...')
    ws.close()
    print('Terminating twisted server...')
    reactor.stop()

#---------------------------------------------------------------------------
def update_spinner():
    msg = 'listening %s' % next(spinner)
    sys.stdout.write(msg)
    sys.stdout.flush()
    sys.stdout.write('\b'*len(msg))
    #time.sleep(1)
