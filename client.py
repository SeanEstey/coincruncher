import json, logging, time
import curses
from curses import wrapper#, newpad

from client_config import *
from app.timer import Timer
from app import analyze, screen, db, views
from app.screen import KEY_UP, KEY_DOWN
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

    log.info("Updated %s", collection)

#----------------------------------------------------------------------
def main(stdscr):
    refresh_delay = 5
    scrollspeed = 5
    scrollpos = scrollremain = 0
    padheight = 200
    scrollscr = None
    screen.setup(stdscr)
    n_lines = screen.get_n_lines()
    n_cols = screen.get_n_cols()

    log.info("--------------------------")
    log.debug("Restarted")
    user_data = json.load(open('data.json'))
    #update_db('watchlist', user_data['watchlist'])
    #update_db('portfolio', user_data['portfolio'])

    timer = Timer()
    fn_show = views.watchlist
    fn_show(stdscr)

    while True:
        ch = screen.input_char(stdscr)

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
            byte_input = screen.input_prompt(stdscr, 10, int(curses.COLS/2), "Enter Symbol")
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
            scrollscr.refresh(scrollpos, 0, 0, 0, n_lines-1, n_cols-1)
        elif ch == KEY_DOWN:
            if fn_show != views.history:
                continue
            scrollpos += min(scrollspeed, scrollremain)
            scrollremain -= min(scrollspeed, scrollremain)
            log.debug('DOWN scroll, pos=%s, remain=%s', scrollpos, scrollremain)
            scrollscr.refresh(scrollpos, 0, 0, 0, n_lines-1, n_cols-1)
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
        log.info('sleep loop')

    screen.teardown(stdscr)
    exit()

# Curses wrapper to take care of setup/teardown
wrapper(main)
