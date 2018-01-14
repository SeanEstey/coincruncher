"""Grabs prices from Coinmarketcap API
https://coinmarketcap.com/api/
"""
import json, getopt, logging, os, queue, signal, sys, time, threading
from app import display, mongo
from app.api import get_markets, get_tickers
from app.display import bcolors, show_watchlist, show_markets, show_portfolio, show_spinner
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
def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

#----------------------------------------------------------------------
def parse_input(linein):
    global watchlist, portfolio

    if linein.find('q') > -1:
        os.kill(os.getpid(), signal.SIGINT)
        exit()
    elif linein.find('m') > -1:
        clear()
        show_markets()
        get_markets()
        clear()
        show_markets()
    elif linein.find('w') > -1:
        clear()
        show_watchlist(watchlist)
        ids = [ n['id'] for n in watchlist ]
        get_tickers(1, 450)
        clear()
        show_watchlist(watchlist)
    elif linein.find('p') > -1:
        clear()
        show_portfolio(portfolio)
        ids = [ n['id'] for n in portfolio ]
        get_tickers(1, 200)
        clear()
        show_portfolio(portfolio)

#----------------------------------------------------------------------
def input_loop():
    """work thread's loop: work on available input until main
    thread exits
    """
    while True:
        try:
            parse_input(input_queue.get(timeout=INPUT_TIMEOUT))
        except queue.Empty:
            pass

#----------------------------------------------------------------------
def cleanup(*args):
    """things to be done before exiting the main thread should go
    in here
    """
    exit()

#----------------------------------------------------------------------
if __name__ == "__main__":
    user_data = json.load(open('data.json'))
    watchlist = user_data['watchlist']
    portfolio = user_data['portfolio']

    print("%sUpdating prices in %s every %ss...%s\n" %(
        bcolors.OKGREEN, CURRENCY, FREQ, bcolors.ENDC))

    # handle sigint, which is being used by the work thread to tell the main thread to exit
    signal.signal(signal.SIGINT, cleanup)
    # will hold all input read, until the work thread has chance to deal with it
    input_queue = queue.Queue()
    work_thread = threading.Thread(target=input_loop)
    work_thread.start()

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
