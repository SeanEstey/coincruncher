# app.coincap

def get_tickers():
    try:
        r = requests.get("https://api.fixer.io/latest?base=USD&symbols=CAD")
    except Exception as e:
        log.warning("Error getting USD/CAD rate")
        pass
    else:
        usd_cad = json.loads(r.text)["rates"]["CAD"]

    # Get Coincap.io market data
    cc_data=None
    try:
        r = requests.get("http://coincap.io/global", headers={'Cache-Control': 'no-cache'})
    except Exception as e:
        log.warning("Error getting Coincap.io market data")
        pass
    else:
        cc_data = json.loads(r.text)
        cc_data['timestamp'] = int(time.time())
        #for n in ['altCap','btcCap','totalCap']:
        #    cc_data[n] *= usd_cad
        db.coincap_global.insert_one(cc_data)

    log.info("Received in %ss" % t1.clock())
