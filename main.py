# main
import getopt
import sys
import logging
import time
from threading import Thread
from queue import Queue
from binance.client import Client
import app
from docs.conf import *
from docs.botconf import *

log = logging.getLogger('main')
divstr = "***** %s *****"
# Global candle queues. Data added by websock thread, accessed by trade thread
q_closed = Queue()
q_open = Queue()

if __name__ == '__main__':
    log.info('Initializing main.')

    killer = app.GracefulKiller()
    app.set_db(host)
    from app.bot import candles, scanner, trade, websock

    # Handle input commands
    try:
        opts, args = getopt.getopt(sys.argv[1:], ":c", ['candles'])
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt not in('-c', '--candles'):
            continue
        pairs = trade.get_enabled_pairs()
        candles.update(pairs, TRADEFREQS)

    client = Client('','')
    trade.init(client_=client)

    threads = []
    for func in [websock.run, trade.run, trade.stoploss, scanner.run]:
        threads.append(Thread(
            name='{}.{}'.format(func.__module__, func.__name__),
            target=func))
        threads[-1].setDaemon(True)
        threads[-1].start()

    n_threads = len(threads)

    while True:
        if killer.kill_now:
            break
        for t in threads:
            if t.is_alive() == False:
                print("Thread {} is dead!".format(t.getName()))
                time.sleep(1)
        time.sleep(0.1)

    print("quitting")
    log.debug(divstr % "sys.exit()")
    log.info(divstr % "Terminating")
    sys.exit()
