""" app.bot.websock
python-binance docs:
http://python-binance.readthedocs.io/en/latest/overview.html
Binance wss docs:
https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md
"""
import logging
import threading
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
from app.bot import lock, get_pairs, candles

from main import q

log = logging.getLogger('websock')
connkeys, storedata = [], []
ws = None

#-------------------------------------------------------------------------------
def run(e_pairs, e_kill):
    global storedata, connkeys, ws
    client = app.bot.client

    #print("Connecting to websocket...")
    ws = BinanceSocketManager(client)

    pairs = get_pairs()
    connkeys += [ws.start_kline_socket(pair, recv_kline, interval=n) \
        for n in TRD_FREQS for pair in pairs]
    lock.acquire()
    print("Subscribed to {} kline sockets.".format(len(connkeys)))
    lock.release()

    ws.start()
    #print('Connected. Press Ctrl+C to quit')

    tmr = Timer(name='pairs', expire='every 5 clock min utc', quiet=True)

    while True:
        if e_kill.isSet():
            break

        if e_pairs.isSet():
            update_sockets()
            e_pairs.clear()

        if tmr.remain() == 0:
            tmr.reset()
            if len(storedata) > 0:
                #print("websock_thread: saving new candles...")
                candles.bulk_save(storedata)
                storedata = []


        time.sleep(1)

    close_all()
    print("Websock thread: Terminating...")

#-------------------------------------------------------------------------------
def update_sockets():
    """conn_key str format: <symbol>@kline_<interval>
    """
    global connkeys, storedata, ws
    log.debug("Websock thread: update_sockets")

    old = set([n[0:n.index('@')].upper() for n in connkeys])
    new = set(app.bot.get_pairs())

    # Removed pairs: close all sockets w/ matching symbols.
    for pair in (old - new):
        for k in connkeys:
            if pair in k:
                ws.stop_socket(k)
                connkeys.pop(k)

    # Added pairs: create sockets for each candle frequency.
    newpairs = new - old
    connkeys += [ws.start_kline_socket(i, recv_kline, interval=j) for j in TRD_FREQS for i in newpairs]

    log.debug("{} pair(s) removed, {} added. {} total sockets."\
        .format(len(old-new), len(newpairs), len(connkeys)))

#-------------------------------------------------------------------------------
def recv_kline(msg):
    """Kline socket callback function. Formats raw candle data, feeds into
    trading queue, adds to global dataframe, and saves to list for periodic
    saving to DB.
    """
    global storedata

    if msg['e'] != 'kline':
        lock.acquire()
        print(msg)
        lock.release()
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

        lock.acquire()
        print("{}{:<7}{}{:>5}{:>12g}{}".format(colors.GRN, candle['pair'], colors.WHITE,
            candle['freqstr'], candle['close'], colors.ENDC))
        lock.release()

    # Send to trade queue.
    q.put(candle)

#-------------------------------------------------------------------------------
def close_all():
    global ws
    sys.stdout.flush()
    print('Websock thread: Closing all sockets...')
    ws.close()
    print('Websock thread: Terminating twisted server...')
    reactor.stop()
