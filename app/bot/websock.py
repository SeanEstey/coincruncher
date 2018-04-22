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
from app.common.utils import colors
from app.common.timeutils import strtofreq
from app.common.timer import Timer
from app.bot import get_pairs, candles
from main import q

spinner = itertools.cycle(['-', '/', '|', '\\'])
connkeys, storedata = [], []
ws = None

#-------------------------------------------------------------------------------
def run(e_pairs):
    global storedata, connkeys, ws
    client = app.bot.client

    print("Connecting to websocket...")
    ws = BinanceSocketManager(client)

    pairs = get_pairs()
    connkeys += [ws.start_kline_socket(pair, recv_kline, interval=n) \
        for n in TRADEFREQS for pair in pairs]
    print("Subscribed to {} kline sockets.".format(len(connkeys)))

    ws.start()
    print('Connected. Press Ctrl+C to quit')

    tmr = Timer(name='pairs', expire='every 5 clock min utc', quiet=True)

    while True:
        if e_pairs.isSet():
            update_sockets()
            e_pairs.clear()
        if tmr.remain() == 0:
            tmr.reset()
            if len(storedata) > 0:
                print("websock_thread: saving new candles...")
                candles.bulk_save(storedata)
                storedata = []
                #app.bot.dfc = candles.bulk_load(get_pairs(), TRADEFREQS, dfm=app.bot.dfc)
        update_spinner()
        time.sleep(0.1)
    close_all()

#-------------------------------------------------------------------------------
def update_sockets():
    """conn_key str format: <symbol>@kline_<interval>
    """
    global connkeys, storedata, ws
    print("Event: enabled pairs changed. Updating websockets...")

    old = set([n[0:n.index('@')].upper() for n in connkeys])
    new = set(app.bot.get_pairs())

    # Removed pairs: close all sockets w/ matching symbols.
    for pair in (old - new):
        for k in connkeys:
            if pair in k:
                ws.stop_socket(k)
                idx = connkeys.index(k)
                connkeys = connkeys[0:idx] + connkeys[idx+1:]
    print("{} pair socket(s) removed.".format(len(old-new)))

    # Added pairs: create sockets for each candle frequency.
    newpairs = new - old
    connkeys += [ws.start_kline_socket(i, recv_kline, interval=j) for j in TRADEFREQS for i in newpairs]
    print("{} pair socket(s) created.".format(len(newpairs)))

#-------------------------------------------------------------------------------
def recv_kline(msg):
    """Kline socket callback function. Formats raw candle data, feeds into
    trading queue, adds to global dataframe, and saves to list for periodic
    saving to DB.
    """
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
        "quote_buy_vol": np.float64(k['Q']),
        "closed": k['x']
    }

    if k['x'] == True:
        storedata.append(candle)
        print("{}{:<7}{}{:>5}{:>12g}{}".format(colors.GRN, candle['pair'], colors.WHITE,
            candle['freqstr'], candle['close'], colors.ENDC))

    # Send to trade queue.
    q.put(candle)

#-------------------------------------------------------------------------------
def close_all():
    global ws
    print('Closing all sockets...')
    ws.close()
    print('Terminating twisted server...')
    reactor.stop()

#-------------------------------------------------------------------------------
def update_spinner():
    msg = 'listening %s' % next(spinner)
    sys.stdout.write(msg)
    sys.stdout.flush()
    sys.stdout.write('\b'*len(msg))
    #time.sleep(1)
