# daemon
import logging, time
from datetime import datetime, timedelta
import app
from app import GracefulKiller
from app.common.timer import Timer
from app.common.utils import utc_dtdate
from app.bot import candles, trade, scanner, tickers
from docs.botconf import trade_pairs as pairs

log = logging.getLogger('daemon')

#---------------------------------------------------------------------------
def _data(now=False):
    cmc = Timer(name='cmc', expire='every 5 clock min utc')
    #if now == True:
    #    #tickers.update(limit=500)

    while True:
        if cmc.remain() == 0:
            #tickers.update(limit=500)
            print('Updated CMC tickers')
            cmc.reset()

        print("cmc: {:} sec remain".format(cmc.remain(unit='s')))
        time.sleep(cmc.remain()/1000)

#---------------------------------------------------------------------------
def _scanner():
    scan_tmr = Timer(name='scanner', expire='every 30 clock min utc')
    n = 3
    scanner.new_scanner()
    #scanner.scan("30m", 8, n, 1000, idx_filter='BTC', quiet=True)
    #scanner.scan("1h", 4, n, 1000, idx_filter='BTC', quiet=True)

    while True:
        if scan_tmr.remain() == 0:
            scanner.new_scanner()
            #scanner.scan("30m", 8, n, 1000, idx_filter='BTC', quiet=True)
            #scanner.scan("1h", 4, n, 1000, idx_filter='BTC', quiet=True)
            scan_tmr.reset()

        time.sleep(300)

#---------------------------------------------------------------------------
def _daily():
    timer_1d = Timer(name='daily', expire=utc_dtdate()+timedelta(days=1))

    while True:
        if timer_1d.remain() == 0:
            app.eod_tasks()
            timer_1d.set_expiry(utc_dtdate() + timedelta(days=1))

        print("daily: {:} sec remain".format(timer_1d.remain(unit='s')))
        time.sleep(timer_1d.remain()/1000)

#---------------------------------------------------------------------------
def _trading():
    """Main trade cycle loop.
    """
    print('Preloading historic data....')
    trade.init()

    timer_5m = Timer(name='trade_5m', expire='every 5 clock min utc')
    timer_30m = Timer(name='trade_30m', expire='every 30 clock min utc')
    timer_1h = Timer(name='trade_1h', expire='every 60 clock min utc')

    while True:
        if timer_1h.remain() == 0:
            tickers.aggregate_mkt(freqstr='1h')
            time.sleep(10)
            trade.update('1h')
            timer_1h.reset()

        if timer_30m.remain() == 0:
            tickers.aggregate_mkt(freqstr='30m')
            time.sleep(10)
            trade.update('30m')
            timer_30m.reset()

        if timer_5m.remain() == 0:
            tickers.aggregate_mkt(freqstr='5m')
            time.sleep(10)
            trade.update('5m')
            timer_5m.reset()

        #if timer_1m.remain() == 0:
        #    time.sleep(8)
        #    trade.update('1m')
        #    timer_1m.reset()

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
            #candles.update(pairs, '1m',
            #    start='24 hours ago utc', force=True)
            candles.update(pairs, '5m',
                start='80 hours ago utc', force=True)
            candles.update(pairs, '30m',
                start='80 hours ago utc', force=True)
            candles.update(pairs, '1h',
                start='80 hours ago utc', force=True)
            candles.update(pairs, '1d',
                start='14 days ago utc', force=True)
        elif opt in('-t', '--tickers'):
            # Preload cmc tickers w/o waiting on timer
            kwargs["force_tick"] = True

    # Create threads
    try:
        #th1 = threading.Thread(
        #    name='data', target=_data, kwargs=th1_kwargs)
        th2 = threading.Thread(
            name='daily', target=_daily)
        th3 = threading.Thread(
            name='trade', target=_trading)
        th4 = threading.Thread(
            name='scanner', target=_scanner)
    except Exception as e:
        log.exception("datathread main()")
        print(str(e))
        sys.exit()

    #th1.setDaemon(True)
    #th1.start()

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
