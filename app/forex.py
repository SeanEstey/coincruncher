# app.forex

import json, logging, pytz, requests
from datetime import datetime, date, timedelta
from app import get_db
from app.utils import duration, utc_datetime, utc_dtdate, utc_date
log = logging.getLogger('forex')

#-------------------------------------------------------------------------------
def rate(currency, date):
    db = get_db()
    result = db.forex_1d.find_one({"date":date})
    return result[currency]

#-------------------------------------------------------------------------------
def update_1d():
    base = 'USD'
    to = 'CAD'
    db = get_db()
    tomorrow = utc_dtdate() + timedelta(days=1)
    _next = tomorrow - utc_datetime()

    # Have we saved today's rates?
    if db.forex_1d.find({"date":utc_dtdate()}).count() > 0:
        log.debug("forex_1d update in %s hrs.", duration(_next, "hours"))
        return duration(_next)

    try:
        uri = "https://api.fixer.io/%s?base=%s&symbols=%s" %(utc_date(),base,to)
        response = requests.get(uri)
    except Exception as e:
        log.exception("error querying forex rates")
        return duration(_next)
    else:
        if response.status_code != 200:
            log.error("forex status=%s, text=%s", response.status_code, response.text)
            return duration(_next)

    # Update
    data = json.loads(response.text)
    db.forex_1d.insert_one({
        "date":utc_dt(today),
        "USD":1,
        "CAD":data["rates"][to]
    })

    log.info("updated forex rates for %s, USD->CAD=%s",
        today, data["rates"][to])

    return duration(_next)

#-------------------------------------------------------------------------------
def update_hist_forex(symbol, start, end):
    """@symbol: fiat currency to to show USD conversion to
    @start, end: datetime objects in UTC
    """
    db = get_db()
    diff = end - start

    for n in range(0,diff.days):
        # TODO: store 'date' and 'CAD' fields in db.forex_1d collection
        dt = start + timedelta(days=1*n)
        print(dt.isoformat())
        uri = "https://api.fixer.io/%s?base=USD&symbols=%s" %(dt.date(),symbol)

#-------------------------------------------------------------------------------
def get_forex(_from, _to, _date):
    response = "https://api.fixer.io/%s?base=%s&symbols=%s" %(_date, _from, _to)
    data = json.loads(response.text)
    return data["rates"][_to]
