# app.daemon

import logging, time, threading, getopt, sys
from config import *
from app import get_db, set_db, tickers, coinmktcap, forex, markets
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
log = logging.getLogger("daemon")

#---------------------------------------------------------------------------
def main():
    while True:
        t = 500
        t = min(t, forex.update_1d()) # Once a day
        t = min(t, coinmktcap.get_tickers_5m())
        t = min(t, coinmktcap.get_marketidx_5m())
        #t = min(t, tickers.update_1d())
        #t = min(t, markets.update_1d()) # Once a day

        log.debug("sleeping %s sec...", t)

        time.sleep(max(t,0))

#---------------------------------------------------------------------------
if __name__ == '__main__':
    log.info("---------- starting daemon ----------")
    log.debug("---------- starting daemon ----------")

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
        time.sleep(0.1)

    log.info("---------- terminating daemon ----------")
    exit()
