# main
import logging
import time
from queue import Queue
import app
from app import GracefulKiller
from app.bot import candles, scanner, trade, websock
log = logging.getLogger('main')

# Global candle queues. Data added by websock thread, accessed by trade thread
q_closed = Queue()
q_open = Queue()

#---------------------------------------------------------------------------
if __name__ == '__main__':
    import getopt
    import threading
    from threading import Thread
    import sys
    from app import set_db
    from docs.conf import host
    from docs.botconf import tradefreqs
    divstr = "***** %s *****"
    log.info('Initializing main.')

    killer = GracefulKiller()
    set_db(host)

    # Handle input commands
    try:
        opts, args = getopt.getopt(sys.argv[1:], ":c", ['candles'])
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt not in('-c', '--candles'):
            continue
        pairs = trade.get_enabled_pairs()
        # Query seed candle data from API
        for n in tradefreqs:
            candles.update(pairs, n,
                start="72 hours ago utc", force=True)

    trade.init()

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
