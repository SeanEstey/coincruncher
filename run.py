import curses, json, logging, requests, signal, time, threading
from curses import wrapper
from datetime import datetime
from app.timer import Timer
from app import display, db
from app.coinmktcap import get_markets, get_tickers
from config import *
from btfxwss import BtfxWss
log = logging.getLogger(__name__)

wss = None
ticker_q = None

#-------------------------------------------------------------------------------
def wss_init():
    global wss
    wss = BtfxWss()
    wss.start()

    while not wss.conn.connected.is_set():
        time.sleep(1)

    log.info("wss initialized")
    # Subscribe to some channels
    wss.subscribe_to_ticker('BTCUSD')
    time.sleep(8)

#-------------------------------------------------------------------------------
def wss_listen():
    global wss, ticker_q
    usd_to_cad = 1.2444

    while True:
        # Accessing data stored in BtfxWss:
        try:
            ticker_q = wss.tickers('BTCUSD')  # returns a Queue object for the pair.
        except Exception as e:
            log.exception("Error getting wss ticker")
            break

        while not ticker_q.empty():
            try:
                tick = ticker_q.get()
            except Exception as e:
                log.exception("Error getting queue item")
                return False
            else:
                if tick is None:
                    log.info("None found in queue. Exiting")
                    return False

            btcusd = tick[0][0]
            db.bitfinex_tickers.insert_one({
                "price_cad": round(btcusd[6] * usd_to_cad, 2),
                "vol_24h_cad": round(btcusd[7] * btcusd[6] * usd_to_cad, 2),
                "pct_24h": round(btcusd[5] * 100, 2),
                "datetime": datetime.fromtimestamp(tick[1])
            })
            log.info("Wss ticker saved")

        time.sleep(0.1)

#-------------------------------------------------------------------------------
def wss_unsub():
    global wss
    wss.unsubscribe_from_ticker('BTCUSD')
    # Shutting down the client:
    wss.stop()

#----------------------------------------------------------------------
def wss_app():
    wss_init()
    wss_listen()
    wss_unsub()

#----------------------------------------------------------------------
def setup_db(collection, data):
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
        get_tickers(0,700)
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
    #exit()

#----------------------------------------------------------------------
def main(stdscr):
    global ticker_q

    setup(stdscr)
    log.info("--------------------------")
    log.info("Crypfolio running!")

    user_data = json.load(open('data.json'))
    setup_db('watchlist', user_data['watchlist'])
    setup_db('portfolio', user_data['portfolio'])

    wss_thread = threading.Thread(name="WssThread", target=wss_app)
    wss_thread.setDaemon(True)
    wss_thread.start()

    data_thread = threading.Thread(name="DataThread", target=update_data)
    data_thread.setDaemon(True)
    data_thread.start()

    refresh_delay = 1
    timer = Timer()

    fn_show = display.watchlist
    fn_show(stdscr)

    while True:
        # Check if thread still alive
        if not data_thread.is_alive():
            log.critical("data_thread is dead!")
            break
        if not wss_thread.is_alive():
            log.critical("wss_thread is dead!")
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
        elif ch == ord('q'):
            log.info('Shutting down queue')
            break

        if timer.clock(stop=False) >= refresh_delay:
            if fn_show:
                timer.restart()
                fn_show(stdscr)
        time.sleep(0.1)

    teardown(stdscr)
    ticker_q.put(None)
    wss_thread.join()
    exit()


# Curses wrapper to take care of setup/teardown
wrapper(main)
