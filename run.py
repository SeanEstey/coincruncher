"""Grabs prices from Coinmarketcap API
https://coinmarketcap.com/api/
"""
import curses, json, getopt, logging, multiprocessing, os, queue, signal, sys, time, threading
from curses import wrapper, init_pair, color_pair
from app import display, mongo
from app.timer import Timer
from app.api import setup_db, update_markets, update_tickers
from app.display import bcolors, show_watchlist, show_markets, show_portfolio
from config import *
log = logging.getLogger(__name__)

#----------------------------------------------------------------------
def parse_input(ch):
    if ch.find('q') > -1:
        os.kill(os.getpid(), signal.SIGINT)
        exit()
    elif ch == 'm': #.find('m') > -1:
        show_markets()
        return show_markets
    elif ch == 'w': #linein.find('w') > -1:
        show_watchlist()
        return show_watchlist
    elif ch == 'p': #linein.find('p') > -1:
        show_portfolio()
        return show_portfolio

#----------------------------------------------------------------------
def input_loop():
    """work thread's loop: work on available input until main
    thread exits
    """
    refresh_timer = Timer()
    last_ptr = None
    log.info("input_loop")

    """
    while True:
        log.info("inside input loop")
        try:
            ptr = parse_input(getch.getch())
            #ptr = parse_input(input_queue.get(timeout=INPUT_TIMEOUT))
            if ptr and ptr != last_ptr:
                last_ptr = ptr
        except queue.Empty:
            pass

        if last_ptr and refresh_timer.clock(stop=False) >= 5:
            log.info("Refreshing screen")
            last_ptr()
            refresh_timer.restart()
    """

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
def setup(stdscr):
    """Setup curses window.
    """
    # create a window object that represents the terminal window
    #stdscr = curses.initscr()
    # enable terminal colors
    curses.start_color()
    init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLACK)
    init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
    init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)
    #curses.use_default_colors()
    # Don't print what I type on the terminal
    curses.noecho()
    # React to every key press, not just when pressing "enter"
    curses.cbreak()
    # Make getch() non-blocking
    stdscr.nodelay(True)
    stdscr.keypad(True)
    # hide cursor
    curses.curs_set(0)
    #return stdscr

#----------------------------------------------------------------------
def teardown(stdscr):
    # Reverse changes made to terminal by cbreak()
    curses.nocbreak()
    stdscr.keypad(False)
    curses.echo()
    # restore the terminal to its original state
    curses.endwin()
    exit()

#----------------------------------------------------------------------
def main(stdscr):
    setup(stdscr)

    log.info("")
    log.info("Crypfolio running!")
    user_data = json.load(open('data.json'))
    setup_db('watchlist', user_data['watchlist'])
    setup_db('portfolio', user_data['portfolio'])

    while True:
        ch = stdscr.getch()
        curses.flushinp()

        if ch == -1:
            pass
        elif ch == ord('p'):
            stdscr.addstr(12,1, "'p' pressed, showing portfolio")
            show_portfolio(stdscr)
        elif ch == ord('m'):
            stdscr.addstr(12,1, "'m' pressed. showing markets")
            show_markets()
        elif ch == ord('w'):
            stdscr.addstr(12,1, "'w' pressed. showing watchlist")
            show_watchlist()
        elif ch == ord('q'):
            stdscr.addstr(12,1 , "'q' pressed. last_ch='%s'. Terminating" % last_ch)
            break
        else:
            stdscr.addstr(12, 1, "'%s' pressed" % str(ch))

        time.sleep(0.5)

    teardown(stdscr)

    # handle sigint, which is being used by the work thread to tell the main thread to exit
    #signal.signal(signal.SIGINT, cleanup)
    # will hold all input read, until the work thread has chance to deal with it
    #input_queue = queue.Queue()

    #data_thread = threading.Thread(name="DataThread", target=update_data)
    #data_thread.start()

    """input_thread = myThread(1, "InputThread", cont)
    input_thread.start()

    while True:
        if cont != []:
            #parse_input(
            log.info("We got it: %s", cont)
            #cont = []
        else:
            log.info("Cont: %s, input_thread.cont=%s", cont, input_thread.cont)
        time.sleep(0.5)
    """
    #input_thread = threading.Thread(target=new_input)
    #input_thread.start()
    #input_thread.join(timeout=0.1)

    # main loop: stuff input in the queue
    #for line in sys.stdin:
    #    log.info("Adding input to queue")
    #    input_queue.put(line)

    #input_thread.join()

wrapper(main)
