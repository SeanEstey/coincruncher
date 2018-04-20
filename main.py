# main
import getopt
import sys
import logging
import time
from threading import Thread, Event
from queue import Queue
from binance.client import Client
from docs.conf import *
from docs.botconf import *
import app, app.bot


##### Globals #####

log = logging.getLogger('main')
divstr = "***** %s *****"
# Candle data queues fed by app.bot.websock thread and consumed
# by app.bot.trade thread.
q_closed = Queue()
q_open = Queue()
e_pairs = Event()

if __name__ == '__main__':
    killer = app.GracefulKiller()
    app.set_db(host)
    app.bot.init()

    from app.bot import candles, scanner, trade, websock

    # Handle input commands
    try:
        opts, args = getopt.getopt(sys.argv[1:], ":c", ['candles'])
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt not in('-c', '--candles'):
            continue
        pairs = app.bot.get_pairs()
        candles.update(pairs, TRADEFREQS)

    # Create worker threads. Set as daemons so they terminate
    # automatically if main process is killed.
    threads = []
    for func in [websock.run, trade.full_klines, trade.part_klines, scanner.run]:
        threads.append(Thread(
            name='{}.{}'.format(func.__module__, func.__name__),
            target=func,
            args=(e_pairs,)
        ))
        threads[-1].setDaemon(True)
        threads[-1].start()

    # Main loop. Monitors threads and terminates app on CTRL+C cmd.
    while True:
        if killer.kill_now:
            break
        for t in threads:
            if t.is_alive() == False:
                print("Thread {} is dead!".format(t.getName()))
                time.sleep(1)
        time.sleep(0.1)

    print("Goodbye")
    log.debug(divstr % "sys.exit()")
    log.info(divstr % "Terminating")
    sys.exit()
