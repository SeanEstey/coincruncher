# app.daemon

import logging, time, threading, getopt, sys
from config import *
from app import get_db, set_db, coinmktcap, forex, markets
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
log = logging.getLogger("daemon")

#---------------------------------------------------------------------------
def main():
    while True:
        t_rem = []
        t_rem += [coinmktcap.update_5m()]
        t_rem += [markets.update_1d()]
        t_rem += [forex.update_1d()]

        # Sleep until time
        log.debug("refresh in %s sec", abs(min(t_rem)))
        time.sleep(abs(min(t_rem)))

#---------------------------------------------------------------------------
if __name__ == '__main__':
    log.info("***** starting daemon *****")

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

    log.info("***** terminating daemon *****")
    exit()
