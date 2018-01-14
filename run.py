"""Grabs prices from Coinmarketcap API
https://coinmarketcap.com/api/
"""
import json, getopt, logging, os, queue, signal, sys, time, threading
from app import display, mongo
from app.timer import Timer
from app.api import setup_db, update_markets, update_tickers
from app.display import bcolors, show_watchlist, show_markets, show_portfolio
from config import *
log = logging.getLogger(__name__)

#----------------------------------------------------------------------
class myThread (threading.Thread):
   def __init__(self, threadID, name, q):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
      self.q = q
   def run(self):
      print ("Starting " + self.name)
      process_data(self.name, self.q)
      print ("Exiting " + self.name)

#----------------------------------------------------------------------
def parse_input(linein):
    if linein.find('q') > -1:
        os.kill(os.getpid(), signal.SIGINT)
        exit()
    elif linein.find('m') > -1:
        show_markets()
        return show_markets
    elif linein.find('w') > -1:
        show_watchlist()
        return show_watchlist
    elif linein.find('p') > -1:
        show_portfolio()
        return show_portfolio

#----------------------------------------------------------------------
def input_loop():
    """work thread's loop: work on available input until main
    thread exits
    """
    refresh_timer = Timer()
    last_ptr = None

    while True:
        try:
            ptr = parse_input(input_queue.get(timeout=INPUT_TIMEOUT))
            if ptr and ptr != last_ptr:
                last_ptr = ptr
        except queue.Empty:
            pass

        if last_ptr and refresh_timer.clock(stop=False) >= 5:
            last_ptr()
            refresh_timer.restart()

#----------------------------------------------------------------------
def update_data():
    while True:
        log.info('Updating tickers...')
        update_tickers(0,700)
        log.info('Updating markets...')
        update_markets()
        log.info('Sleeping 60s...')
        time.sleep(60)

#----------------------------------------------------------------------
def cleanup(*args):
    """things to be done before exiting the main thread should go
    in here
    """
    exit()

#----------------------------------------------------------------------
if __name__ == "__main__":
    user_data = json.load(open('data.json'))
    setup_db('watchlist', user_data['watchlist'])
    setup_db('portfolio', user_data['portfolio'])

    print("%sUpdating prices in %s every %ss...%s\n" %(
        bcolors.OKGREEN, CURRENCY, FREQ, bcolors.ENDC))

    # handle sigint, which is being used by the work thread to tell the main thread to exit
    signal.signal(signal.SIGINT, cleanup)
    # will hold all input read, until the work thread has chance to deal with it
    input_queue = queue.Queue()
    work_thread = threading.Thread(target=input_loop)
    work_thread.start()

    data_thread = threading.Thread(target=update_data)
    data_thread.start()

    # main loop: stuff input in the queue
    for line in sys.stdin:
        input_queue.put(line)

    # wait for work thread to finish
    work_thread.join()

    """try:
        argv = sys.argv[1:]
        opts, args = getopt.getopt(argv,"mwp", ['markets', 'watchlist', 'portfolio'])
    except getopt.GetoptError:
        sys.exit(2)

    if set(['-w','--watchlist','-p','--portfolio']).intersection(set([n[0] for n in opts])):
    for opt, arg in opts:
        if opt in ('-m', '--markets'):
        elif opt in ('-w', '--watchlist'):
        elif opt in ('-p', '--portfolio'):
    """
