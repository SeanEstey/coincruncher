# daemon
import logging, time, signal
from datetime import datetime, timedelta
import app
from app import GracefulKiller
from app.common.timer import Timer
from app.common.utils import utc_dtdate
from app.bnc import candles, trade
from app.cmc import tickers
from app.common import forex
from docs.config import TICKER_LIMIT
from docs.data import BINANCE

log = logging.getLogger('daemon')

#---------------------------------------------------------------------------
def data(now=False):
    """
    """
    cmc = Timer(name='cmc', expire='every 5 clock min utc')
    if now == True:
        tickers.update(limit=500)

    while True:
        if cmc.remain() == 0:
            tickers.update(limit=500)
            print('Updated CMC tickers')
            cmc.reset()

        print("cmc: {:} sec remain".format(cmc.remain(unit='s')))
        time.sleep(cmc.remain()/1000)

#---------------------------------------------------------------------------
def daily():
    """
    """
    timer_1d = Timer(name='daily', expire=utc_dtdate()+timedelta(days=1))

    while True:
        if timer_1d.remain() == 0:
            app.eod_tasks()
            timer_1d.set_expiry(utc_dtdate() + timedelta(days=1))

        print("daily: {:} sec remain".format(timer_1d.remain(unit='s')))
        time.sleep(timer_1d.remain()/1000)

#---------------------------------------------------------------------------
def trading():
    """Main trade cycle loop.
    """
    pairs = BINANCE['PAIRS']
    timer_1m = Timer(name='trade', expire='every 1 clock min utc')
    print('Preloading historic data....')
    trade.init()

    while True:
        if timer_1m.remain() == 0:
            # Wait ~7s until websocket data has streamed in
            time.sleep(7)
            trade.update('1m')
            timer_1m.reset()

        time.sleep(5)

#---------------------------------------------------------------------------
if __name__ == '__main__':
    import getopt
    import threading
    import sys
    from app import set_db

    divstr = "***** %s *****"
    log.info('Initializing daemon.')
    killer = GracefulKiller()

    try:
        opts, args = getopt.getopt(
            sys.argv[1:], "h:ct", ['dbhost=', 'candles', 'tickers'])
    except getopt.GetoptError:
        sys.exit(2)

    th1_kwargs = {}
    th2_kwargs = {}

    for opt, arg in opts:
        if opt in('-h', '--dbhost'):
            set_db(arg)
        elif opt in('-c', '--candles'):
            # Preload binance candles w/o waiting on timer
            candles.update(pairs, '1m',
                start='4 hours ago utc', force=True)
            candles.update(pairs, '5m',
                start='4 hours ago utc', force=True)
            candles.update(pairs, '1h',
                start='36 hours ago utc', force=True)
        elif opt in('-t', '--tickers'):
            # Preload cmc tickers w/o waiting on timer
            kwargs["force_tick"] = True

    # Create threads
    try:
        th1 = threading.Thread(
            name='data', target=data, kwargs=th1_kwargs)
        th2 = threading.Thread(
            name='daily', target=daily) #kwargs=kwargs)
        th3 = threading.Thread(
            name='trade', target=trading) #, kwargs=kwargs)
    except Exception as e:
        log.exception("datathread main()")
        print(str(e))
        sys.exit()

    th1.setDaemon(True)
    th1.start()

    th2.setDaemon(True)
    th2.start()

    th3.setDaemon(True)
    th3.start()

    print("starting loop")

    while True:
        if killer.kill_now:
            break
        time.sleep(0.1)

    print("quitting")

    log.debug(divstr % "sys.exit()")
    log.info(divstr % "Terminating")
    sys.exit()
