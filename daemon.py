# app.daemon

import logging, time, threading, getopt, pytz, sys
from datetime import datetime
from config import *
from app import get_db, set_db
from app.coinmktcap import updt_markets, updt_tickers
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
log = logging.getLogger("daemon")

#---------------------------------------------------------------------------
def update_data():
    """Query and store coinmarketcap market/ticker data. Sync data fetch with
    CMC 5 min update frequency.
    """
    # Fetch coinmarketcap data every 5 min
    CMC_UPDT_FREQ = 300

    # Update daily ticker historical data at end of each day.
    # Use closing price
    UPDT_HIST_TCKR_FREQ = 3600 * 24

    db = get_db()

    while True:
        updated_dt = list(db.market.find().sort('_id',-1).limit(1))[0]['date']
        now = datetime.utcnow().replace(tzinfo=pytz.utc)
        t_remain = int(CMC_UPDT_FREQ - (now - updated_dt).total_seconds())

        if t_remain <= 0:
            updt_tickers(0,1500)
            updt_markets()
            log.debug("data refresh in %s sec.", CMC_UPDT_FREQ)
            time.sleep(60)
        else:
            log.debug("data refresh in %s sec.", t_remain)
            time.sleep(min(t_remain, 60))

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

    data_thread = threading.Thread(name="DataThread", target=update_data)
    data_thread.setDaemon(True)
    data_thread.start()

    while True:
        time.sleep(0.1)

    log.info("***** terminating daemon *****")
    exit()
