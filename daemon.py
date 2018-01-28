import logging, time, threading, inspect
from pprint import pformat
from server_config import *
from app import analyze, db
from app.coinmktcap import update_markets, update_tickers

log = logging.getLogger(__name__)

#----------------------------------------------------------------------
def getAttributes(obj):
    result = ''
    for name, value in inspect.getmembers(obj):
        if callable(value) or name.startswith('__'):
            continue
        result += pformat("%s: %s" %(name, value)) + "\n"
    return result

#----------------------------------------------------------------------
def update_data():
    while True:
        update_tickers(0,1500)
        update_markets()
        time.sleep(90)

if __name__ == '__main__':
    data_thread = threading.Thread(name="DataThread", target=update_data)
    data_thread.setDaemon(True)
    data_thread.start()
