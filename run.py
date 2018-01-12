"""Grabs prices from Coinmarketcap API, writes them to Default.js file in Numi
extension directory.
Docs: https://coinmarketcap.com/api/
"""
import json, getopt, os, queue, signal, sys, time, threading
from api import get_markets, _get_ticker, get_ticker, get_tickers
from display import bcolors, show_watchlist, show_markets, show_portfolio, show_spinner

freq = 30
currency = "cad"
timeout = 0.1 # seconds

#----------------------------------------------------------------------
def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

#----------------------------------------------------------------------
def parse_input(linein):
    global watchlist, portfolio
    print("parse_input()")

    #try:
    if linein.find('q') > -1:
        os.kill(os.getpid(), signal.SIGINT)
        exit()
    elif linein.find('m') > -1:
        print("Showing markets...")
        market_data = get_markets(currency)
        #clear()
        show_markets(market_data, currency)
    elif linein.find('w') > -1:
        print("Showing watchlist...")
        ids = [ n['id'] for n in watchlist ]
        ticker_data = get_ticker(ids, currency)
        #clear()
        show_watchlist(watchlist, ticker_data, currency)
    elif linein.find('p') > -1:
        print("Showing portfolio...")
        ids = [ n['id'] for n in portfolio ]
        ticker_data = get_tickers(100, currency)
        #clear()
        show_portfolio(portfolio, ticker_data, currency)
    #except Exception as e:
    #    print("input excepton")
    #    pass

#----------------------------------------------------------------------
def input_loop():
    # work thread's loop: work on available input until main
    # thread exits
    while True:

        try:
            parse_input(input_queue.get(timeout=timeout))
        except queue.Empty:
            pass

#----------------------------------------------------------------------
def cleanup(*args):
    # things to be done before exiting the main thread should go
    # in here
    exit()

#----------------------------------------------------------------------
if __name__ == "__main__":
    user_data = json.load(open('data.json'))
    watchlist = user_data['watchlist']
    portfolio = user_data['portfolio']

    print("%sUpdating prices in %s every %ss...%s\n" % (bcolors.OKGREEN, currency, freq, bcolors.ENDC))

    # handle sigint, which is being used by the work thread to tell the main thread to exit
    signal.signal(signal.SIGINT, cleanup)
    # will hold all input read, until the work thread has chance to deal with it
    input_queue = queue.Queue()
    work_thread = threading.Thread(target=input_loop)
    work_thread.start()

    # main loop: stuff input in the queue
    for line in sys.stdin:
        print("main loop")
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
