# For deprecated btfxwss websocket package

from btfxwss import BtfxWss
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


wss_thread = threading.Thread(name="WssThread", target=wss_app)
wss_thread.setDaemon(True)
wss_thread.start()
    while True:
        # Check if thread still alive
        if not data_thread.is_alive():
            log.critical("data_thread is dead!")
            break
        if not wss_thread.is_alive():
            log.critical("wss_thread is dead!")
            break
ticker_q.put(None)
wss_thread.join()
exit()
