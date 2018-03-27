# app.cmc

#---------------------------------------------------------------------------
def mkt_db_audit():
    """Verifies the completeness of the db collections, generates any
    necessary documents to fill gaps if possible.
    """
    return False
    db = get_db()
    log.debug("DB: verifying...")

    # Verify cmc_mkt completeness
    market_1d = db.cmc_mkt.find().sort('date',-1).limit(1)
    last_date = list(market_1d)[0]["date"]

    n_days = utc_dtdate() - timedelta(days=1) - last_date
    log.debug("DB: cmc_mkt size: {:,}".format(market_1d.count()))

    for n in range(0,n_days.days):
        generate_1d(last_date + timedelta(days=n+1))

#------------------------------------------------------------------------------
def tkr_db_audit():
    # Verify cmc_tick completeness
    db = get_db()

    _tickers = db.cmc_tick.aggregate([
        {"$group":{
            "_id":"$id",
            "date":{"$max":"$date"},
            "name":{"$last":"$name"},
            "symbol":{"$last":"$symbol"},
            "rank":{"$last":"$rank_now"},
            "count":{"$sum":1}
        }},
        {"$sort":{"rank":1}}
    ])
    _tickers = list(_tickers)

    log.debug("%s aggregated ticker_1d assets", len(_tickers))

    for tckr in _tickers:
        last_update = utc_dtdate() - delta(days=1) - tckr["date"]
        if last_update.total_seconds() < 1:
            log.debug("%s up-to-date.", tckr["symbol"])
            continue

        log.debug("updating %s (%s out-of-date)", tckr["symbol"], last_update)

        get_history(
            tckr["_id"],
            tckr["name"],
            tckr["symbol"],
            tckr["rank"],
            tckr["date"],
            utc_dtdate())

    log.debug("DB: verified")
