# daemon
import logging, time, signal
from datetime import datetime, timedelta
from app.timer import Timer
from app.utils import utc_dtdate
from app import candles, coinmktcap, forex, markets, trades
from docs.config import TICKER_LIMIT
from docs.data import BINANCE
log = logging.getLogger('daemon')

#---------------------------------------------------------------------------
class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        self.kill_now = True
#---------------------------------------------------------------------------
def main(force_tick=False, force_candle=False):
    """Main trade cycle loop. Fetch candle data at timed intervals,
    update trading positions.
    """
    pairs = BINANCE['pairs']

    timer_1m = Timer(name='1mTimer', expire='every 1 clock min utc')
    timer_5m = Timer(name='5mTimer', expire='every 5 clock min utc')
    timer_hr = Timer(name='1hTimer', expire='next hour change')
    timer_1d = Timer(name='1dTimer', expire=utc_dtdate()+timedelta(days=1))

    markets.db_audit()
    trades.init()

    if force_tick == True:
        coinmktcap.tickers(limit=TICKER_LIMIT)
        coinmktcap.global_markets()

    if force_candle == True:
        candles.update(pairs, '1m', start='4 hours ago utc', force=True)
        candles.update(pairs, '5m', start='1 hours ago utc', force=True)
        candles.update(pairs, '1h', start='4 hours ago utc', force=True)
        trades.update('5m')

    # Main loop
    while True:
        if timer_1m.remain() == 0:
            candles.update(pairs, '1m')
            trades.update('1m')
            timer_1m.reset()

        if timer_5m.remain() == 0:
            candles.update(pairs, '5m')
            coinmktcap.global_markets()
            trades.update('5m')
            coinmktcap.tickers(limit=500)
            timer_5m.reset()

        if timer_hr.remain() == 0:
            candles.update(pairs, '5m')
            candles.update(pairs, '1h')
            trades.update('1h')
            timer_hr.set_expiry('next hour change')

        if timer_1d.remain() == 0:
            candles.update(pairs, '1d')
            trades.update('1h')
            eod_tasks()
            timer_1d.set_expiry(utc_dtdate() + timedelta(days=1))

        time.sleep(5)
#---------------------------------------------------------------------------
def eod_tasks():
    import os
    from docs.mongo_key import DBUSER, DBPASSWORD, AUTHDB
    forex.update_1d()
    yday = utc_dtdate() - timedelta(days=1)
    markets.generate_1d(yday)
    log.debug('running mongodump...')
    os.system("mongodump -u %s -p %s -d crypto -o ~/Dropbox/mongodumps \
        --authenticationDatabase %s" %(DBUSER, DBPASSWORD, AUTHDB))
    log.info('eod tasks completed')
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
        opts, args = getopt.getopt(sys.argv[1:], "h:ct", ['dbhost=', 'candles', 'tickers'])
    except getopt.GetoptError:
        sys.exit(2)

    kwargs={}
    for opt, arg in opts:
        if opt in('-h', '--dbhost'):
            set_db(arg)
        # Preload binance candles w/o waiting on timer
        elif opt in('-c', '--candles'):
            kwargs["force_candle"] = True
        # Preload cmc tickers w/o waiting on timer
        elif opt in('-t', '--tickers'):
            kwargs["force_tick"] = True

    try:
        data_thread = threading.Thread(
            name="DataThread",
            target=main,
            kwargs=kwargs
        )
    except Exception as e:
        log.exception("datathread main()")
        sys.exit()

    data_thread.setDaemon(True)
    data_thread.start()

    while True:
        if killer.kill_now:
            break
        time.sleep(0.1)

    log.debug(divstr % "sys.exit()")
    log.info(divstr % "Terminating")
    sys.exit()
