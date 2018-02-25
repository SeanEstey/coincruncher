# daemon
import logging, time, signal
from datetime import timedelta
from app import forex
from app.utils import utc_dtdate
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
def main():
    from config import TICKER_LIMIT, BINANCE_CANDLE_PAIRS
    from binance.client import Client
    from app import candles, coinmktcap, markets
    from app.timer import Timer

    #tickers.db_audit()
    markets.db_audit()
    tmr_eod = Timer(expire=utc_dtdate()+timedelta(days=1))

    while True:
        waitfor=[300]
        waitfor.append(coinmktcap.get_tickers_5t(limit=TICKER_LIMIT))
        waitfor.append(coinmktcap.get_marketidx_5t())

        for pair in BINANCE_CANDLE_PAIRS:
            df = candles.to_df(
                pair,
                candles.api_get(pair, "5m", "10 minutes ago UTC"),
                store_db=True
            )

            log.info("%s %s candle(s) updated (Binance)", len(df), pair)

        if tmr_eod.remaining() == 0:
            eod_tasks()
            tmr_eod.set_expiry(utc_dtdate() + timedelta(days=1))

        t_nap = min([n for n in waitfor if type(n) is int])
        log.debug("sleeping (%ss)", t_nap)
        time.sleep(t_nap)

#---------------------------------------------------------------------------
if __name__ == '__main__':
    import getopt
    import threading
    import sys
    from app import set_db

    # STFU
    logging.getLogger("requests").setLevel(logging.ERROR)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

    divstr = "##### %s #####"
    log.info(divstr % "restarted")
    log.debug(divstr % "restarted")
    killer = GracefulKiller()

    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:", ['dbhost='])
    except getopt.GetoptError:
        sys.exit(2)

    for opt, arg in opts:
        if opt in('-h', '--dbhost'):
            set_db(arg)

    data_thread = threading.Thread(name="DataThread", target=main)
    data_thread.setDaemon(True)
    data_thread.start()

    while True:
        if killer.kill_now:
            break
        time.sleep(0.1)

    log.debug(divstr % "sys.exit()")
    log.info(divstr % "exiting")
    sys.exit()

