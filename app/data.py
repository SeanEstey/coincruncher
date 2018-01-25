from datetime import datetime, timedelta
from dateutil import tz


def fill_forex(symbol, start, end):
    """@symbol: fiat currency to to show USD conversion to
    @start, end: datetime objects in UTC
    """
    diff = end - start

    for n in range(0,diff.days):
        dt = start + timedelta(days=1*n)
        print(dt.isoformat())

        uri = "https://api.fixer.io/%s?base=USD&symbols=%s" %(dt.date(),symbol)

        # TODO: store 'date' and 'CAD' fields in db.forex_historical collection

