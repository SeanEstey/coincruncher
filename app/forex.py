
#-------------------------------------------------------------------------------
def update_hist_forex(symbol, start, end):
    """@symbol: fiat currency to to show USD conversion to
    @start, end: datetime objects in UTC
    """
    db = get_db()
    diff = end - start

    for n in range(0,diff.days):
        # TODO: store 'date' and 'CAD' fields in db.forex_historical collection
        dt = start + timedelta(days=1*n)
        print(dt.isoformat())
        uri = "https://api.fixer.io/%s?base=USD&symbols=%s" %(dt.date(),symbol)

#-------------------------------------------------------------------------------
def get_forex(_from, _to, _date):
    response = "https://api.fixer.io/%s?base=%s&symbols=%s" %(_date, _from, _to))
    data = json.loads(response.text)
    return data["rates"][_to]
