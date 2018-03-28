"""
python-binance docs:
http://python-binance.readthedocs.io/en/latest/overview.html

Binance wss docs:
https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md
"""
import logging
import itertools
import signal
import time
import sys
import pandas as pd
import numpy as np
from app.common.utils import utc_datetime as now
from twisted.internet import reactor
from pprint import pprint
from binance.client import Client
from binance.websockets import BinanceSocketManager
from binance.enums import *
from docs.data import BINANCE
from app import set_db, get_db
from app.common.utils import to_local, utc_datetime as now
import app.bnc

pairs = BINANCE['PAIRS']
spinner = itertools.cycle(['-', '/', '|', '\\'])
conn_keys = []
bnc_wss = None

#---------------------------------------------------------------------------
class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        self.kill_now = True

#---------------------------------------------------------------------------
def receive_kline(msg):
    if msg['e'] != 'kline':
        print('not a kline')
        return
    # unclosed candle
    elif msg['k']['x'] != True:
        return
    else:
        c = msg['k']

    doc = {
        'pair': c['s'],
        'freq': c['i'],
        'open_time': pd.to_datetime(c['t'], unit='ms', utc=True),
        'close_time': pd.to_datetime(c['T'], unit='ms', utc=True),
        'open': np.float64(c['o']),
        'high': np.float64(c['h']),
        'low': np.float64(c['l']),
        'close': np.float64(c['c']),
        'trades': c['n'],
        'volume': np.float64(c['v']),
        'buy_vol': np.float64(c['V']),
        'quote_vol': np.float64(c['q']),
        'sell_vol': np.float64(c['Q'])
    }

    if doc['volume'] > 0:
        doc['buy_ratio'] = np.float64(doc['buy_vol'] / doc['volume'])
    else:
        doc['buy_ratio'] = np.float64(0.0)

    print("{:%H:%M:%S:} {} {}: {:.8f}".format(
        to_local(doc['close_time']), doc['freq'], doc['pair'], doc['close']))

    db.candles.insert_one(doc)

#---------------------------------------------------------------------------
def connect_klines(bnc_wss):
    global conn_keys

    print("Creating kline connections...")

    for pair in pairs:
        conn_keys += [
            bnc_wss.start_kline_socket(pair, receive_kline, interval=KLINE_INTERVAL_1MINUTE),
            bnc_wss.start_kline_socket(pair, receive_kline, interval=KLINE_INTERVAL_5MINUTE),
            bnc_wss.start_kline_socket(pair, receive_kline, interval=KLINE_INTERVAL_1HOUR),
            bnc_wss.start_kline_socket(pair, receive_kline, interval=KLINE_INTERVAL_1DAY)
        ]

#---------------------------------------------------------------------------
def close_all():
    print('Closing all sockets...')
    bnc_wss.close()
    print('Terminating twisted server...')
    reactor.stop()

#---------------------------------------------------------------------------
def update_spinner():
    """
    """
    msg = 'listening %s' % next(spinner)
    sys.stdout.write(msg)
    sys.stdout.flush()
    sys.stdout.write('\b'*len(msg))
    #time.sleep(1)

#---------------------------------------------------------------------------
if __name__ == '__main__':
    db = set_db('localhost')
    cred = list(db.api_keys.find())[0]

    killer = GracefulKiller()

    print("Connecting to Binance websocket client...")
    client = Client(cred['key'], cred['secret'])
    bnc_wss = BinanceSocketManager(client)
    connect_klines(bnc_wss)
    bnc_wss.start()

    print('Connected.')
    print('Press Ctrl+C to quit')

    while True:
        if killer.kill_now:
            print('Caught SIGINT command. Shutting down...')

            break
        update_spinner()
        time.sleep(0.1)

    close_all()
