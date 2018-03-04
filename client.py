# client
import curses, logging
from docs.config import *
from app.timer import Timer
from app import set_db, get_db, screen, views
from curses import KEY_UP, KEY_DOWN
log = logging.getLogger("client")

# Globals
scrollpos = scrollremain = 0
scrollscr = None
view = None
timer = Timer()

#----------------------------------------------------------------------
def update_db(collection, data):
    # Initialize if collection empty
    db = get_db()
    if db[collection].find().count() == 0:
        for item in data:
            db[collection].insert_one(item)
            log.info('initialized %s symbol %s', collection, item['symbol'])
    # Update collection
    else:
        for item in data:
            db[collection].replace_one({'symbol':item['symbol']}, item, upsert=True)
            log.debug('updated %s symbol %s', collection, item['symbol'])

        symbols = [ n['symbol'] for n in data ]
        for doc in db[collection].find():
            if doc['symbol'] not in symbols:
                log.debug('deleted %s symbol %s', collection, item['symbol'])
                db[collection].delete_one({'_id':doc['_id']})

    log.info("updated %s", collection)

#----------------------------------------------------------------------
def process_input(stdscr, ch):
    global scrollpos, scrollscr, scrollremain, timer, view

    if ch == ord(','):
        view = views.show_home
        view(stdscr)
    elif ch == ord('p'):
        view = views.show_portfolio
        view(stdscr)
        stdscr.refresh()
    elif ch == ord('s'):
        view = views.show_signals
        view(stdscr)
        stdscr.refresh()
    elif ch == ord('d'):
        view = views.show_patterns
        view(stdscr)
        stdscr.refresh()
    elif ch == ord('m'):
        view = views.show_markets
        view(stdscr)
        stdscr.refresh()
    elif ch == ord('w'):
        view = views.show_watchlist
        view(stdscr)
        stdscr.refresh()
    elif ch == ord('h'):
        stdscr.clear()
        byte_input = screen.input_prompt(stdscr, 10, int(curses.COLS/2), "Enter Symbol")
        symbol = byte_input.decode('utf-8').upper()
        scrollscr = curses.newpad(DISP_PAD_HEIGHT, curses.COLS-1)
        scrollpos = 0
        scrollremain = views.show_history(scrollscr, symbol)
        scrollscr.refresh(scrollpos, 0, 0, 0, curses.LINES-1, curses.COLS-1)
        view = views.show_history
    elif ch == KEY_UP:
        if view != views.show_history:
            return False
        scrollremain += min(DISP_SCROLL_SP, scrollpos)
        scrollpos -= min(DISP_SCROLL_SP, scrollpos)
        log.debug('UP scroll, pos=%s, remain=%s', scrollpos, scrollremain)
        scrollscr.refresh(scrollpos, 0, 0, 0, n_lines-1, n_cols-1)
    elif ch == KEY_DOWN:
        if view != views.show_history:
            return False
        scrollpos += min(DISP_SCROLL_SP, scrollremain)
        scrollremain -= min(DISP_SCROLL_SP, scrollremain)
        log.debug('DOWN scroll, pos=%s, remain=%s', scrollpos, scrollremain)
        scrollscr.refresh(scrollpos, 0, 0, 0, n_lines-1, n_cols-1)

    if timer.elapsed() < DISP_REFRESH_DELAY or view is None:
        return False

    timer.reset()

    if view != views.show_history:
        view(stdscr)

#----------------------------------------------------------------------
def main(stdscr):
    from docs.data import COINMARKETCAP as CMC
    import json
    import getopt
    import sys
    import time
    global view

    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            "h:pw",
            ['dbhost=', 'portfolio', 'watchlist'])
    except getopt.GetoptError:
        sys.exit(2)

    log.info("----- client started -----")
    log.debug("client started,  cmd opts=%s, args=%s", opts, args)

    for opt, arg in opts:
        if opt in('-h', '--dbhost'):
            set_db(arg)
        elif opt in ('-p', '--portfolio'):
            update_db('portfolio',
                [{"symbol":k,"amount":v} for k,v in CMC["PORTFOLIO"].items()])
        elif opt in('-w', '--watchlist'):
            update_db('watchlist',
                [{"symbol":n} for n in CMC["WATCH"]])

    log.debug("initializing curses screen")
    screen.setup(stdscr)
    n_lines = screen.get_n_lines()
    n_cols = screen.get_n_cols()

    view = views.show_home
    view(stdscr)
    ch = None

    while ch != ord('q'):
        ch = screen.input_char(stdscr)
        process_input(stdscr, ch)
        time.sleep(0.1)

    log.info("exiting client")
    screen.teardown(stdscr)
    exit()

if __name__ == "__main__":
    from curses import wrapper
    # Curses wrapper to take care of setup/teardown
    wrapper(main)
