# daemon
import logging, time, signal
from datetime import datetime, timedelta
import app
from app import GracefulKiller
from app.common.timer import Timer
from app.common.utils import utc_dtdate
from app.bnc import analyze, candles, trade
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
def analysis():
    analyze.top_performers(10, idx_filter='BTC')

    anal = Timer(name='analysis', expire='every 60 clock min utc')

    while True:
        if anal.remain() == 0:
            analyze.top_performers(10, idx_filter='BTC')
            anal.reset()
            time.sleep(3500)

        time.sleep(5)

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

    print('Preloading historic data....')
    trade.init()

    timer_1m = Timer(name='trade_1m', expire='every 1 clock min utc')
    timer_5m = Timer(name='trade_5m', expire='every 5 clock min utc')

    while True:
        if timer_5m.remain() == 0:
            trade.update('5m')
            timer_5m.reset()

        if timer_1m.remain() == 0:
            time.sleep(8)
            trade.update('1m')
            timer_1m.reset()

        time.sleep(5)

#---------------------------------------------------------------------------
if __name__ == '__main__':
    import getopt
    import threading
    import sys
    from app import set_db
    pairs = BINANCE['PAIRS']

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
                start='24 hours ago utc', force=True)
            candles.update(pairs, '5m',
                start='100 hours ago utc', force=True)
            candles.update(pairs, '1h',
                start='72 hours ago utc', force=True)
            candles.update(pairs, '1d',
                start='7 days ago utc', force=True)
        elif opt in('-t', '--tickers'):
            # Preload cmc tickers w/o waiting on timer
            kwargs["force_tick"] = True

    # Create threads
    try:
        th1 = threading.Thread(
            name='data', target=data, kwargs=th1_kwargs)
        th2 = threading.Thread(
            name='daily', target=daily)
        th3 = threading.Thread(
            name='trade', target=trading)
        th4 = threading.Thread(
            name='analyze', target=analysis)
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

    th4.setDaemon(True)
    th4.start()

    print("starting loop")

    while True:
        if killer.kill_now:
            break
        time.sleep(0.1)

    print("quitting")

    log.debug(divstr % "sys.exit()")
    log.info(divstr % "Terminating")
    sys.exit()
