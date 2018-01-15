import curses, json, logging, time, threading
from curses import wrapper
from app.timer import Timer
from app.api import setup_db, update_markets, update_tickers
from app.display import set_colors, show_watchlist, show_markets, show_portfolio
from config import *
log = logging.getLogger(__name__)

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
    set_colors(stdscr)
    # Don't print what I type on the terminal
    curses.noecho()
    # React to every key press, not just when pressing "enter"
    curses.cbreak()
    # Make getch() non-blocking
    stdscr.nodelay(True)
    stdscr.keypad(True)
    # hide cursor
    curses.curs_set(0)
    stdscr.refresh()

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
    log.info("--------------------------")
    log.info("Crypfolio running!")
    user_data = json.load(open('data.json'))
    setup_db('watchlist', user_data['watchlist'])
    setup_db('portfolio', user_data['portfolio'])
    data_thread = threading.Thread(name="DataThread", target=update_data)
    data_thread.start()

    while True:
        refresh_timer = Timer()
        ch = stdscr.getch()
        curses.flushinp()

        if ch == -1:
            pass
        elif ch == ord('p'):
            show_portfolio(stdscr)
        elif ch == ord('m'):
            show_markets(stdscr)
        elif ch == ord('w'):
            show_watchlist(stdscr)
        elif ch == ord('q'):
            log.info('Terminating')
            break
        else:
            log.info('Invalid input key %s' % str(ch))

        time.sleep(0.5)

    teardown(stdscr)

    exit()
    data_thread.join()

wrapper(main)
