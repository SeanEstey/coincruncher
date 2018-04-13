"""
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
from app.common.utils import utc_datetime as now
from twisted.internet import reactor
from binance.client import Client
from binance.websockets import BinanceSocketManager
from binance.enums import *
import docs.botconf
from docs.botconf import trade_pairs as pairs
from app import GracefulKiller, set_db, get_db
from app.common.utils import colors, to_local, utc_datetime as now
from app.common.timer import Timer
import app.bot

spinner = itertools.cycle(['-', '/', '|', '\\'])
conn_keys = []
bnc_wss = None

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

    color = None
    if doc['freq'] == '5m':
        color = colors.GRN
    elif doc['freq'] == '1h' or doc['freq'] == '1d':
        color = colors.BLUE
    else:
        color = colors.WHITE

    print("{}{:%H:%M:%S:} {:<7} {:>5} {:>12g}{}"\
        .format(
            colors.WHITE,
            to_local(doc['close_time']),
            doc['pair'],
            doc['freq'],
            doc['close'],
            #doc['volume'],
            colors.ENDC
        ))

    db.candles.insert_one(doc)

#---------------------------------------------------------------------------
def detect_pair_change():
    """Detect changes in pairs tracking conf
    """
    importlib.reload(docs.botconf)
    from docs.botconf import trade_pairs
    global pairs, conn_keys

    if pairs == trade_pairs:
        return pairs
    else:
        print("Detected change in trading pair(s).")
        rmvd = set(pairs) - set(trade_pairs)
        added = set(trade_pairs) - set(pairs)

        if len(rmvd) > 0:
            print("Removing {}...".format(rmvd))
            n_rmv = 0

            for pair in rmvd:
                name = str(pair).lower()

                for key in conn_keys:
                    if name in key:
                        # remove it
                        bnc_wss.stop_socket(key)
                        idx = conn_keys.index(key)
                        conn_keys = conn_keys[0:idx] + conn_keys[idx+1:]
                        n_rmv += 1

            print("Removed {} websocket(s)".format(n_rmv))

        if len(added) > 0:
            print("Adding {}...".format(added))
            n_added = 0

            for pair in added:
                n_added += connect_klines(bnc_wss, [str(pair)])
            print("Added {} websocket(s)".format(n_added))

        #print("Done. {} connections.".format(len(conn_keys)))

        pairs = trade_pairs
        return pairs

#---------------------------------------------------------------------------
def connect_klines(bnc_wss, _pairs):
    global conn_keys

    print("Creating kline connections for: {}...".format(_pairs))

    n_connected = 0

    for pair in _pairs:
        conn_keys += [
            #bnc_wss.start_kline_socket(pair, receive_kline,
            #    interval=KLINE_INTERVAL_1MINUTE),
            bnc_wss.start_kline_socket(pair, receive_kline,
                interval=KLINE_INTERVAL_5MINUTE),
            bnc_wss.start_kline_socket(pair, receive_kline,
                interval=KLINE_INTERVAL_30MINUTE),
            bnc_wss.start_kline_socket(pair, receive_kline,
                interval=KLINE_INTERVAL_1HOUR),
            bnc_wss.start_kline_socket(pair, receive_kline,
                interval=KLINE_INTERVAL_1DAY)
        ]
        n_connected += 4

    return n_connected

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
    connect_klines(bnc_wss, pairs)
    print("{} connections created.".format(len(conn_keys)))
    bnc_wss.start()

    print('Connected.')
    print('Press Ctrl+C to quit')

    timer_1m = Timer(name='pairs', expire='every 1 clock min utc')

    while True:
        if timer_1m.remain(quiet=True) == 0:
            pairs = detect_pair_change()
            timer_1m.reset(quiet=True)

        if killer.kill_now:
            print('Caught SIGINT command. Shutting down...')
            break
        update_spinner()
        time.sleep(0.1)

    close_all()
