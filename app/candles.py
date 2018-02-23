# app.candles

import dateparser, logging, json, time, pytz
from pprint import pformat, pprint
from datetime import datetime, timedelta as delta, date
import pandas as pd
from binance.client import Client
from app import get_db
from app.timer import Timer
from app.utils import utc_datetime, utc_dtdate, utc_date, to_float, to_int, to_dt
log = logging.getLogger('binance')

#------------------------------------------------------------------------------
def historical(pair, interval, start_str, end_str=None):
    """Get Historical Klines from Binance
    If using offset strings for dates add "UTC" to date string e.g. "now UTC",
    "11 hours ago UTC"
    :param symbol: Name of symbol pair e.g BNBBTC
    :type symbol: str
    :param interval: Binance Kline interval
    :type interval: str
        e.g.: Client.KLINE_INTERVAL_30MINUTE, Client.KLINE_INTERVAL_1WEEK
    :param start_str: Start date string in UTC format
    :type start_str: str
        e.g: "1 Dec, 2017", "1 day ago UTC"
    :param end_str: optional - end date string in UTC format
    :type end_str: str
    :return: list of OHLCV value
    """
    client = Client("", "")
    result = []
    limit = 500
    timeframe = intrvl_to_ms(interval)
    start_ts = date_to_ms(start_str)
    end_ts = date_to_ms(end_str) if end_str else None
    idx = 0

    while True:
        data = client.get_klines(symbol=pair, interval=interval, limit=limit,
            startTime=start_ts, endTime=end_ts)
        if len(data) > 0:
            result += data
            start_ts = data[len(data) - 1][0] + timeframe
        else:
            start_ts += timeframe
        idx += 1

        # Test limits/prevent API spamming
        if len(data) < limit:
            break
        if idx % 3 == 0:
            time.sleep(1)
    return result

#------------------------------------------------------------------------------
def to_dict(data):
    """Convert each serial candle list->dict. List format:
        [0]: open time (timestamp): str
        [1]: open price: str
        [2]: high price: str
        [3]: low price: str
        [4]: close price: str
        [5]: base orderbook vol (lpair): str
        [6]: close time (timestamp): str
        [7]: quote orderbook vol (rpair): str
        [8]: n_trades: int
        [9]: taker buy vol (ask order executed, gain l-pair sym): str
        [10]: taker sell vol (ask order executed, give r-pair sym): str
        [11]: (ignore)
    """
    candles = []
    for candle in data:
        candles.append({
            "date": to_dt(candle[0]/1000),
            "open": float(candle[1]),
            "high": float(candle[2]),
            "low": float(candle[3]),
            "close": float(candle[4]),
            "base_ob_vol": float(candle[5]),
            "quote_ob_vol": float(candle[7]),
            "trades": candle[8],
            "base_buy_vol": float(candle[9]),
            "quote_sell_vol": float(candle[10])
        })
    return candles

#------------------------------------------------------------------------------
def to_df(pair, rawdata):
    df = pd.DataFrame(
        index=[ to_dt(n[0]/1000) for n in rawdata ],
        data=[[pair, float(x[1]),float(x[2]),float(x[3]),float(x[4]),x[8],float(x[5]),
                float(x[9]), float(x[10]), float(x[7])] for x in rawdata],
        columns=["pair", "open","high","low","close","trades","base_ob_vol",
                 "base_buy_vol", "quote_sell_vol", "quote_ob_vol"]
    )
    df.index.name = "date"
    return df

#------------------------------------------------------------------------------
def date_to_ms(date_str):
    """Convert UTC date to milliseconds
    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours
    ago UTC"
    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/
    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours
    ago UTC", "now UTC"
    :type date_str: str
    """
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    d = dateparser.parse(date_str)
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)
    return int((d - epoch).total_seconds() * 1000.0)

#------------------------------------------------------------------------------
def intrvl_to_ms(interval):
    """Convert a Binance interval string to milliseconds
    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h,
    6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str
    :return:
         None if unit not one of m, h, d or w
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }
    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            pass
    return ms
