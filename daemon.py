# daemon
from pprint import pformat
import logging, time, signal
from datetime import timedelta
from app import forex, markets
from app.utils import utc_dtdate, utc_datetime
log = logging.getLogger("daemon")

#---------------------------------------------------------------------------
class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        self.kill_now = True

#---------------------------------------------------------------------------
def eod_tasks():
    import os

    forex.update_1d()
    yday = utc_dtdate() - timedelta(days=1)
    #tickers.generate_1d(yday)
    markets.generate_1d(yday)
    log.debug("running mongodump...")
    os.system("mongodump -d crypto -o ~/Dropbox/mongodumps")
    log.info("eod tasks completed")

#---------------------------------------------------------------------------
def main(tckr=None, cndl=None):
    print("main")
    from docs.config import TICKER_LIMIT
    from docs.data import BINANCE
    from binance.client import Client
    from app import candles, coinmktcap, signals
    from app.timer import Timer
    from datetime import datetime

    #tickers.db_audit()
    markets.db_audit()
    daily = Timer(name="DailyTimer", expire=utc_dtdate()+timedelta(days=1))
    hourly = Timer(name="HourTimer", expire="next hour change")
    short = Timer(name="MinTimer", expire="in 5 min utc")

    if cndl:
        pairs = BINANCE["CANDLES"]
        candles.api_get_all(pairs, "5m", "6 hours ago utc")
        candles.api_get_all(pairs, "1h", "80 hours ago utc")
        candles.api_get_all(pairs, "1d", "30 days ago utc")
        signals.calculate_all()

    if tckr:
        try:
            coinmktcap.get_tickers_5t(limit=TICKER_LIMIT)
            coinmktcap.get_marketidx_5t()
        except Exception as e:
            log.exception(str(e))
            pass

    signals.calculate_all()

    while True:
        waitfor=[10]

        if short.remain() <= 0:
            try:
                waitfor.append(coinmktcap.get_tickers_5t(limit=TICKER_LIMIT))
                waitfor.append(coinmktcap.get_marketidx_5t())
            except Exception as e:
                log.exception(str(e))
                pass
            candles.api_get_all(BINANCE["CANDLES"], "5m", "1 hour ago utc")
            signals.calculate_all()
            short.set_expiry("in 5 min utc")
        else:
            print("%s: %s" % (short.name, short.remain(unit='str')))

        if hourly.remain() == 0:
            candles.api_get_all(BINANCE["CANDLES"], "1h", "4 hours ago utc")
            candles.api_get_all(BINANCE["CANDLES"], "1d", "2 days ago utc")
            hourly.set_expiry("next hour change")
            signals.calculate_all()
        else:
            print("%s: %s" % (hourly.name, hourly.remain(unit='str')))

        if daily.remain() == 0:
            eod_tasks()
            daily.set_expiry(utc_dtdate() + timedelta(days=1))

        t_nap = min([n for n in waitfor if type(n) is int])
        time.sleep(t_nap)

#---------------------------------------------------------------------------
if __name__ == '__main__':
    import getopt
    import threading
    import sys
    from app import set_db

    divstr = "##### %s #####"
    log.info(divstr % "restarted")
    log.debug(divstr % "restarted")
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
            kwargs["cndl"] = True
        # Preload cmc tickers w/o waiting on timer
        elif opt in('-t', '--tickers'):
            kwargs["tckr"] = True

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
    log.info(divstr % "exiting")
    sys.exit()
