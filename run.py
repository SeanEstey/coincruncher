import curses, json, logging, requests, signal, time, threading
from curses import wrapper, KEY_UP, KEY_DOWN, KEY_ENTER
from datetime import datetime
from app.timer import Timer
from app import analyze, display, db
from app.coinmktcap import get_markets, get_tickers
from config import *

log = logging.getLogger(__name__)

#----------------------------------------------------------------------
def update_db(collection, data):
    # Initialize if collection empty
    if db[collection].find().count() == 0:
        for item in data:
            db[collection].insert_one(item)
            log.info('Initialized %s symbol %s', collection, item['symbol'])
    # Update collection
    else:
        for item in data:
            db[collection].replace_one({'symbol':item['symbol']}, item, upsert=True)
            log.debug('Updated %s symbol %s', collection, item['symbol'])

        symbols = [ n['symbol'] for n in data ]
        for doc in db[collection].find():
            if doc['symbol'] not in symbols:
                log.debug('Deleted %s symbol %s', collection, item['symbol'])
                db[collection].delete_one({'_id':doc['_id']})

    log.info("DB updated w/ user data")

#----------------------------------------------------------------------
def update_data():
    while True:
        log.info('Updating tickers...')
        get_tickers(0,1500)
        log.info('Updating markets...')
        get_markets()
        log.info('Sleeping 60s...')
        time.sleep(60)

#----------------------------------------------------------------------
def setup(stdscr):
    """Setup curses window.
    """
    display.set_colors(stdscr)
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

#----------------------------------------------------------------------
def main(stdscr):
    global ticker_q

    setup(stdscr)
    log.info("--------------------------")
    log.info("Crypfolio running!")

    user_data = json.load(open('data.json'))
    update_db('user_watchlist', user_data['watchlist'])
    update_db('user_portfolio', user_data['portfolio'])

    data_thread = threading.Thread(name="DataThread", target=update_data)
    data_thread.setDaemon(True)
    data_thread.start()

    refresh_delay = 1
    timer = Timer()

    fn_show = display.watchlist
    fn_show(stdscr)

    while True:
        if not data_thread.is_alive():
            log.critical("data_thread is dead!")
            break

        ch = stdscr.getch()
        curses.flushinp()

        if ch == ord('p'):
            timer.restart()
            fn_show = display.portfolio
            fn_show(stdscr)
        elif ch == ord('m'):
            timer.restart()
            fn_show = display.markets
            fn_show(stdscr)
        elif ch == ord('w'):
            timer.restart()
            fn_show = display.watchlist
            fn_show(stdscr)
        elif ch == ord('d'):
            analyze.mktcap()
        elif ch == KEY_UP or ch == KEY_DOWN or ch == KEY_ENTER:
            log.info("Key up/down/enter")
        elif ch == ord('q'):
            log.info('Shutting down queue')
            break

        if timer.clock(stop=False) >= refresh_delay:
            if fn_show:
                timer.restart()
                fn_show(stdscr)
        time.sleep(0.1)

    teardown(stdscr)
    exit()


# Curses wrapper to take care of setup/teardown
wrapper(main)
