import curses, getopt, json, logging, sys, time
from curses import wrapper
from config import *
from app.timer import Timer
from app import screen, set_db, get_db, views
from app.screen import KEY_UP, KEY_DOWN

log = logging.getLogger("client")
refresh_delay = 30
scrollspeed = 5
scrollpos = scrollremain = 0
padheight = 200
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

    if ch == ord('p'):
        log.info("switching to portfolio view")
        view = views.portfolio
        view(stdscr)
        stdscr.refresh()
    elif ch == ord('m'):
        log.info("switching to market view")
        view = views.markets
        view(stdscr)
        stdscr.refresh()
    elif ch == ord('w'):
        log.info("switching to watchlist view")
        view = views.watchlist
        view(stdscr)
        stdscr.refresh()
    elif ch == ord('h'):
        log.info("switching to ticker view")
        stdscr.clear()
        byte_input = screen.input_prompt(stdscr, 10, int(curses.COLS/2), "Enter Symbol")
        symbol = byte_input.decode('utf-8').upper()
        scrollscr = curses.newpad(padheight, curses.COLS-1)
        scrollpos = 0
        scrollremain = views.history(scrollscr, symbol)
        scrollscr.refresh(scrollpos, 0, 0, 0, curses.LINES-1, curses.COLS-1)
        view = views.history
    elif ch == KEY_UP:
        if view != views.history:
            return False
        scrollremain += min(scrollspeed, scrollpos)
        scrollpos -= min(scrollspeed, scrollpos)
        log.debug('UP scroll, pos=%s, remain=%s', scrollpos, scrollremain)
        scrollscr.refresh(scrollpos, 0, 0, 0, n_lines-1, n_cols-1)
    elif ch == KEY_DOWN:
        if view != views.history:
            return False
        scrollpos += min(scrollspeed, scrollremain)
        scrollremain -= min(scrollspeed, scrollremain)
        log.debug('DOWN scroll, pos=%s, remain=%s', scrollpos, scrollremain)
        scrollscr.refresh(scrollpos, 0, 0, 0, n_lines-1, n_cols-1)

    if timer.clock(stop=False) < refresh_delay or view is None:
        return False

    timer.restart()
    #log.debug("refresh timer reset")

    if view != views.history:
        view(stdscr)

#----------------------------------------------------------------------
def main(stdscr):
    global view

    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:pw", ['dbhost=', 'portfolio', 'watchlist'])
    except getopt.GetoptError:
        sys.exit(2)

    log.info("----- client started -----")
    log.debug("client started,  cmd opts=%s, args=%s", opts, args)

    for opt, arg in opts:
        if opt in('-h', '--dbhost'):
            set_db(arg)
        elif opt in ('-p', '--portfolio'):
            user_data = json.load(open('data.json'))
            update_db('portfolio', user_data['portfolio'])
        elif opt in('-w', '--watchlist'):
            user_data = json.load(open('data.json'))
            update_db('watchlist', user_data['watchlist'])

    log.debug("initializing curses screen")
    screen.setup(stdscr)
    n_lines = screen.get_n_lines()
    n_cols = screen.get_n_cols()
    view = views.markets
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
    # Curses wrapper to take care of setup/teardown
    wrapper(main)
