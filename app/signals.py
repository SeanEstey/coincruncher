# app.signals
import logging
from pprint import pprint
import pandas as pd
from app import get_db
log = logging.getLogger('signals')

#-----------------------------------------------------------------------------
def sigstr(candle, hist_df):
    """Compare candle fields to historical averages. Measure magnitude in
    number of standard deviations from the mean.
    """
    print("\n\tCANDLE PAIR: %s" % candle["pair"])
    print("\tCANDLE DATE: %s\n" % candle["date"])

    for field in ["buy_vol","volume","close"]:
        desc = hist_df.describe()
        _min = desc[field]["min"]
        std = desc[field]["std"]
        _max = desc[field]["max"]
        value = candle[field]
        ratio = value/std

        print("\tFIELD: \"%s\"" % field)
        print("\tVALUE: %s" % value)
        print("\tHISTORIC MIN: %.5f" % _min)
        print("\tHISTORIC STD: %.5f" % std)
        print("\tHISTORIC MAX: %.5f" % _max)
        print("\tVALUE/STD: %+.2fx" % ratio)

        if ratio < 1:
            print("\tSIGNAL: WEAK")
        elif ratio > 1 and ratio < 3:
            print("\tSIGNAL: AVERAGE")
        elif ratio >= 3:
            print("\tSIGNAL: STRONG")

        print("")

#------------------------------------------------------------------------------
def coinfi(coin, start, end):
    """It’s simple supply and demand: a sudden increase in the number of buyers
    over sellers in a short space of time. In this scenario, certain phenomena
    can be observed such as:
        -An acceleration of volume within a short period of time.
        -An increase in price volatility over historic averages.
        -An increase in the ratio of volume traded on bid/ask versus normal.
    Each of these phenomena can be quantified by tracking the following factors:
        -Volume traded over an hourly period.
        -Change in price over an hourly period.
        -Volume of the coin traded on the offer divided by the volume of the
        coin traded on the bid over an hourly period.

    Every factor is stored by CoinFi (in this case hourly) and plotted on a
    normal distribution. When the latest hourly data is updated, the model
    reruns comparing the current factor value versus the historical factor
    values. The data is plotted on a normal distribution and any current
    factor value that is 2 standard deviations above the mean triggers an alert.

    When there are multiple factors triggering alerts, they indicate more
    confidence in the signal.

    Example: Volume traded in the last hour was 3 standard deviations above the mean; AND
    Change in price over the last hour is 2 standard deviations above the mean; AND
    Volume ratio of offer/bid is 2 standard deviations above the mean.
    we will have greater confidence in the signal.

    The model will take in factors over multiple time periods (i.e. minute,
    hourly, daily, weekly) and will also compare against multiple historical
    time periods (i.e. volume over last 24 hours, 48 hours, 72 hours)

    Furthermore CoinFi stores these factors across a selected universe of coins
    and the model is continuously run, outputting alerts when a factor or a
    series of factors has a positive signal.
    """
    pass

#------------------------------------------------------------------------------
def custom(coin, start, end):
    """Feb-2018 NANO bull run:
    Bull signals:
        vol_24h_pct >= 2%
        pct_1h > 0%
        pct_7d < -10%
    Bear signals:
        vol_24h_pct <= -2%
        pct_1h > 10%
        pct_7d > 0%
    """
    cursor = get_db().tickers_5t.find(
        {"symbol":coin, "date":{"$gte":start,"$lt":end}},
        {"_id":0,"name":0,"total_supply":0,"circulating_supply":0,"max_supply":0,
        "rank":0,"id":0, "symbol":0}
        ).sort("date",1)
    coin = list(cursor)
    df = pd.DataFrame(coin)
    df.index = df["date"]
    del df["date"]
    df["vol_24h_pct"] = df["vol_24h_usd"].pct_change().round(2) * 100
    return df
