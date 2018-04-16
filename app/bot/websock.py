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
from app.common.utils import utc_datetime as now
from twisted.internet import reactor
from binance.client import Client
from binance.websockets import BinanceSocketManager
import docs.botconf
from docs.botconf import trade_pairs as pairs
from app import GracefulKiller, set_db, get_db
from app.common.utils import colors, to_local, utc_datetime as now
from app.common.timer import Timer
import app.bot

spinner = itertools.cycle(['-', '/', '|', '\\'])
conn_keys = []
ws = None

#---------------------------------------------------------------------------
def mainloop(pairs, freqstrlist):
    db = set_db('localhost')
    cred = list(db.api_keys.find())[0]
    killer = GracefulKiller()

    print("Connecting to Binance websocket client...")
    client = Client(cred['key'], cred['secret'])
    ws = BinanceSocketManager(client)
    n_conn = connect_klines(ws, pairs, freqstrlist)
    print("{} connections created.".format(n_conn))

    ws.start()

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

#---------------------------------------------------------------------------
def sub_klines(ws, pairlist, freqstrlist):
    """Subscribe to pair/freq sockets.
    """
    global conn_keys
    print("Creating kline connections for: {}...".format(_pairs))
    conn_keys += [
        ws.start_kline_socket(pair, recv_kline, interval=n) \
            for n in freqstrlist for pair in pairlist]
    return len(conn_keys)

#---------------------------------------------------------------------------
def recv_kline(msg):
    if msg['e'] != 'kline':
        print('not a kline')
        return

    c = msg['k']
    pair = c['s']
    freqstr = c['i']
    freq = strtofreq(freqstr)
    open_time = pd.to_datetime(c['t'], unit='ms', utc=True)
    close_time = pd.to_datetime(c['T'], unit='ms', utc=True)

    if float(c['v']) > 0:
        buy_ratio = np.float64(c['V']/c['v'])
    else:
        buy_ratio = np.float64(0.0)

    arr = [
        np.float64(c['o']),         # open
        np.float64(c['c']),         # close
        np.float64(c['h']),         # high
        np.float64(c['l']),         # low
        c['n'],                     # trades
        np.float64(c['v']),         # volume
        buy_ratio,
        c['x']                      # partial
    ]

    # Update in-memory candles dataframe
    dfc = app.bot.dfc
    idx = dfc.loc[pair, freq].iloc[-1].name
    # Completed candle. Update existing dataframe row.
    if c['x'] == True:
        df.ix[(pair, freq, idx)] = arr
    # Partial candle. Check if exists, if not, add
    # new row to dataframe
    else:
        df.ix[(pair, freq, idx)] = arr

    """
    # Print output
    color = None
    if doc['freq'] == '5m':
        color = colors.GRN
    elif doc['freq'] == '1h' or doc['freq'] == '1d':
        color = colors.BLUE
    else:
        color = colors.WHITE

    print("{}{:%H:%M:%S:} {:<7} {:>5} {:>12g} {:>7}{}"\
        .format(colors.WHITE, to_local(doc['close_time']),  doc['pair'],
            doc['freq'], doc['close'], 'Closed' if msg['k']['x'] else 'Open',
            colors.ENDC))
    """

#---------------------------------------------------------------------------
def save_klines():
    """Bulk write all new kline data to mongodb. Should be callled on a
    timer every ~60sec.
    """
    pass

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
                        ws.stop_socket(key)
                        idx = conn_keys.index(key)
                        conn_keys = conn_keys[0:idx] + conn_keys[idx+1:]
                        n_rmv += 1

            print("Removed {} websocket(s)".format(n_rmv))

        if len(added) > 0:
            print("Adding {}...".format(added))
            n_added = 0

            for pair in added:
                n_added += connect_klines(ws, [str(pair)])
            print("Added {} websocket(s)".format(n_added))

        #print("Done. {} connections.".format(len(conn_keys)))

        pairs = trade_pairs
        return pairs

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
