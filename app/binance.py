# app.binance

import logging, json
from pprint import pformat, pprint
from datetime import datetime, timedelta as delta, date
import pandas as pd
from app import get_db
from app.timer import Timer
from app.utils import utc_datetime, utc_dtdate, utc_date, to_float, to_int, to_dt
log = logging.getLogger('binance')

from bitex import Binance

def pairs(obj):
    return sorted(obj.supported_pairs)

def init():
    log.debug("creating binance auth api")
    api = get_db().api_keys.find_one({"name":"Binance"})
    binance = Binance(key=api["key"], secret=api["secret"])
    return binance

def ticker(obj, pair):
    log.debug("retrieving %s ticker", pair)
    data = json.loads(obj.ticker(pair).text)

    data.update({
        "askPrice": float(data["askPrice"]),
        "askQty": float(data["askQty"]),
        "bidPrice": float(data["bidPrice"]),
        "bidQty": float(data["bidQty"]),
        "closeTime": to_dt(data["closeTime"]/1000), #datetime.utcfromtimestamp(data["closeTime"]/1000),
        "highPrice": float(data["highPrice"]),
        "lastPrice": float(data["lastPrice"]),
        "lastQty": float(data["lastQty"]),
        "lowPrice": float(data["lowPrice"]),
        "openPrice": float(data["openPrice"]),
        "openTime":  to_dt(data["openTime"]/1000), #datetime.utcfromtimestamp(data["openTime"]/1000),
        "prevClosePrice": float(data["prevClosePrice"]),
        "priceChange": float(data["priceChange"]),
        "priceChangePercent": float(data["priceChangePercent"]),
        "quoteVolume": float(data["quoteVolume"]),
        "volume": float(data["volume"]),
        "weightedAvgPrice": float(data["weightedAvgPrice"])
    })
    return data
