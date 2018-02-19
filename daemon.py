# app.daemon

import logging, time, threading, getopt, os, sys
from datetime import timedelta
from config import *
from app import get_db, set_db, tickers, coinmktcap, forex, markets
from app.timer import Timer
from app.utils import utc_dtdate, utc_datetime
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
log = logging.getLogger("daemon")

#---------------------------------------------------------------------------
def eod_tasks():
    forex.update_1d()
    yday = utc_dtdate() - timedelta(days=1)
    #tickers.generate_1d(yday)
    markets.generate_1d(yday)
    log.debug("running mongodump...")
    os.system("mongodump -d crypto -o ~/Dropbox/mongodumps")
    log.info("eod tasks completed")

#---------------------------------------------------------------------------
def main():
    #tickers.db_audit()
    markets.db_audit()
    t_eod = Timer(expire=utc_dtdate()+timedelta(days=1))

    while True:
        t = 500
        t = min(t, forex.update_1d()) # Once a day
        t = min(t, coinmktcap.get_tickers_5m())
        t = min(t, coinmktcap.get_marketidx_5m())

        if t_eod.remaining() == 0:
            eod_tasks()
            t_eod.set_expiry(utc_dtdate() + timedelta(days=1))

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
