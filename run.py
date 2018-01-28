import curses, json, logging, time, threading, inspect
from pprint import pprint, pformat
from curses import wrapper, KEY_UP, KEY_DOWN, KEY_ENTER
from datetime import datetime
from app.timer import Timer
from app import analyze, display, db, views
from app.coinmktcap import get_markets, get_tickers
from config import *

log = logging.getLogger(__name__)

def getAttributes(obj):
    result = ''
    for name, value in inspect.getmembers(obj):
        if callable(value) or name.startswith('__'):
            continue
        result += pformat("%s: %s" %(name, value)) + "\n"
    return result

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

    log.info("Updated %s", collection)

#----------------------------------------------------------------------
def update_data():
    while True:
        get_tickers(0,1500)
        get_markets()
        time.sleep(90)

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
def my_raw_input(stdscr, y, x, prompt_string):
    stdscr.nodelay(False)
    curses.echo()
    stdscr.addstr(y, x, prompt_string)
    stdscr.refresh()
    input = stdscr.getstr(y+1, x, 20)

    stdscr.nodelay(True)
    curses.noecho()

    return input

#----------------------------------------------------------------------
def main(stdscr):
    global ticker_q
    scrollscr = None
    refresh_delay = 5
    scrollspeed = 5
    scrollpos = 0
    scrollremain = 0
    padheight = 200

    setup(stdscr)
    log.info("--------------------------")
    log.debug("Restarted")
    user_data = json.load(open('data.json'))
    update_db('watchlist', user_data['watchlist'])
    update_db('portfolio', user_data['portfolio'])
    data_thread = threading.Thread(name="DataThread", target=update_data)
    data_thread.setDaemon(True)
    data_thread.start()

    timer = Timer()
    fn_show = views.watchlist
    fn_show(stdscr)

    while True:
        if not data_thread.is_alive():
            log.critical("data_thread is dead!")
            break

        # Poll input
        ch = stdscr.getch()
        curses.flushinp()

        if ch == ord('p'):
            fn_show = views.portfolio
            fn_show(stdscr)
        elif ch == ord('m'):
            fn_show = views.markets
            fn_show(stdscr)
        elif ch == ord('w'):
            fn_show = views.watchlist
            fn_show(stdscr)
        elif ch == ord('h'):
            stdscr.clear()
            byte_input = my_raw_input(stdscr, 10, int(curses.COLS/2), "Enter Symbol")
            symbol = byte_input.decode('utf-8').upper()
            scrollscr = curses.newpad(padheight, curses.COLS-1)
            scrollpos = 0
            scrollremain = views.history(scrollscr, symbol)
            scrollscr.refresh(scrollpos, 0, 0, 0, curses.LINES-1, curses.COLS-1)
            fn_show = views.history
        elif ch == KEY_UP:
            if fn_show != views.history:
                continue
            scrollremain += min(scrollspeed, scrollpos)
            scrollpos -= min(scrollspeed, scrollpos)
            log.debug('UP scroll, pos=%s, remain=%s', scrollpos, scrollremain)
            scrollscr.refresh(scrollpos, 0, 0, 0, curses.LINES-1, curses.COLS-1)
        elif ch == KEY_DOWN:
            if fn_show != views.history:
                continue
            scrollpos += min(scrollspeed, scrollremain)
            scrollremain -= min(scrollspeed, scrollremain)
            log.debug('DOWN scroll, pos=%s, remain=%s', scrollpos, scrollremain)
            scrollscr.refresh(scrollpos, 0, 0, 0, curses.LINES-1, curses.COLS-1)
        elif ch == ord('q'):
            break

        if timer.clock(stop=False) >= refresh_delay:
            if fn_show:
                timer.restart()

                if fn_show == views.history:
                    log.debug("Not redrawing history buf")
                    continue
                else:
                    fn_show(stdscr)
        time.sleep(0.1)

    teardown(stdscr)
    exit()


# Curses wrapper to take care of setup/teardown
wrapper(main)
