""" app.bot.websock
python-binance docs:
http://python-binance.readthedocs.io/en/latest/overview.html
Binance wss docs:
https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md
"""
import logging
import itertools
import importlib
import time
import sys
import pandas as pd
import numpy as np
from twisted.internet import reactor
from binance.client import Client
from binance.websockets import BinanceSocketManager
from docs.botconf import tradefreqs
from app import get_db
from app.common.utils import colors as c
from app.common.timer import Timer
import app.bot
from .trade import get_enabled_pairs
from main import q_open, q_closed

spinner = itertools.cycle(['-', '/', '|', '\\'])
conn_keys = []
storedata = []
ws = None

#---------------------------------------------------------------------------
def run():
    global storedata
    print("Connecting to Binance websocket client...")

    cred = list(app.get_db().api_keys.find())[0]
    client = Client(cred['key'], cred['secret'])
    ws = BinanceSocketManager(client)
    pairs = get_enabled_pairs()
    sub_klines(ws, pairs, tradefreqs)
    ws.start()

    print('Connected. Press Ctrl+C to quit')

    tmr = Timer(name='pairs', expire='every 5 clock min utc')

    while True:
        if tmr.remain(quiet=True) == 0:
            tmr.reset(quiet=True)

            if len(storedata) > 0:
                print("SAVING CANDLES TO DB")
                storedata = []

        update_spinner()
        time.sleep(0.1)

    close_all()

#---------------------------------------------------------------------------
def sub_klines(ws, pairlist, freqstrlist):
    global conn_keys
    print("Creating kline connections for: {}...".format(pairlist))
    conn_keys += [
        ws.start_kline_socket(pair, recv_kline, interval=n) \
            for n in freqstrlist for pair in pairlist]
    print("{} connections created.".format(len(conn_keys)))
    return len(conn_keys)

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
