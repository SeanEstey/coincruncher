import logging, time, threading, getopt, pytz, sys
from datetime import datetime, timedelta
from pprint import pformat
from config import *
from app import get_db, set_db
from app.utils import parse_period
from app.coinmktcap import update_markets, update_tickers
log = logging.getLogger(__name__)

def update_data():
    db = get_db()
    qty, unit, t_refresh = parse_period("5M")

    while True:
        update_tickers(0,1500)
        updated_dt = update_markets()

        t_wait = (updated_dt + t_refresh) - datetime.utcnow()
        log.info("next update in %s sec...", t_wait.seconds)
        if t_wait.seconds > 0:
            time.sleep(t_wait.seconds)

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
