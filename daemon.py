import logging, time, threading, getopt, sys
from pprint import pformat
from config import *
from app import set_db
from app.coinmktcap import update_markets, update_tickers
log = logging.getLogger(__name__)

def update_data():
    while True:
        update_tickers(0,1500)
        update_markets()
        time.sleep(90)

if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:", ['dbhost='])
    except getopt.GetoptError:
        sys.exit(2)

    log.info("----- client started -----")
    log.debug("opts=%s, args=%s", opts, args)

    for opt, arg in opts:
        if opt in('-h', '--dbhost'):
            set_db(arg)

    data_thread = threading.Thread(name="DataThread", target=update_data)
    data_thread.setDaemon(True)
    data_thread.start()

    log.info("Daemon started")

    while True:
        time.sleep(0.1)

    exit()
