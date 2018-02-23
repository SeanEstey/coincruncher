# app.daemon
import logging, time, threading, getopt, os, sys
from datetime import timedelta
from app import get_db, set_db, candles, tickers, coinmktcap, forex, markets
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
    from config import TICKER_LIMIT
    from binance.client import Client
    from app.candles import historical, to_df, store

    #tickers.db_audit()
    markets.db_audit()
    tmr_eod = Timer(expire=utc_dtdate()+timedelta(days=1))

    while True:
        waitfor=[500]
        waitfor.append(coinmktcap.get_tickers_5t(limit=TICKER_LIMIT))
        waitfor.append(coinmktcap.get_marketidx_5t())

        for pair in ["BTCUSDT","NANOETH"]:
            df = to_df(pair, historical(pair, Client.KLINE_INTERVAL_5MINUTE,
                "10 minutes ago UTC"))
            store(df)

        if tmr_eod.remaining() == 0:
            eod_tasks()
            tmr_eod.set_expiry(utc_dtdate() + timedelta(days=1))

        t_nap = min([n for n in waitfor if type(n) is int])
        log.debug("sleeping %s sec...", t_nap)
        time.sleep(t_nap)

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
