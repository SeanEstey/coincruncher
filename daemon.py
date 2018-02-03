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
        t_rem += [coinmktcap.update()]
        t_rem += [markets.aggregate()]
        t_rem += [forex.update()]

        # Sleep until time
        log.debug("next update in %s sec", min(t_rem))
        time.sleep(min(t_rem))

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
