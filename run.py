import curses, json, logging, signal, time, threading
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
    data_thread.setDaemon(True)
    data_thread.start()

    refresh_delay = 5
    timer = Timer()

    fn_show = show_markets
    fn_show(stdscr)

    while True:
        ch = stdscr.getch()
        curses.flushinp()

        if ch == ord('p'):
            timer.restart()
            fn_show = show_portfolio
            fn_show(stdscr)
        elif ch == ord('m'):
            timer.restart()
            fn_show = show_markets
            fn_show(stdscr)
        elif ch == ord('w'):
            timer.restart()
            fn_show = show_watchlist
            fn_show(stdscr)
        elif ch == ord('q'):
            log.info('Terminating')
            break

        if timer.clock(stop=False) >= refresh_delay:
            if fn_show:
                #log.info('Refreshing view')
                timer.restart()
                fn_show(stdscr)
        time.sleep(0.1)

    teardown(stdscr)
    data_thread.join()

# Curses wrapper to take care of setup/teardown
wrapper(main)
