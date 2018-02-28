# app.signals
import logging
from datetime import timedelta
from pprint import pprint
import pandas as pd
from app import get_db
from app.candles import db_get
from app.utils import utc_datetime as now
log = logging.getLogger('signals')

#------------------------------------------------------------------------------
def multisigstr(pair, freq):
    """Average signal strength from 3 different historical average
    time spans.
    """
    tspan=None

    if freq == "1h":
        tspan = timedelta(days=1)
    elif freq == "5m":
        tspan = timedelta(hours=1)

    scores = [
        sigstr(pair, freq, now() - tspan*3, end=now())["score"],
        sigstr(pair, freq, now() - tspan*2, end=now())["score"],
        sigstr(pair, freq, now() - tspan*1, end=now())["score"]
    ]

    avg_score = round(sum(scores) / len(scores), 2)
    print("\nAVERAGE SCORE: %s" % avg_score)

#-----------------------------------------------------------------------------
def sigstr(pair, freq, start, end):
    """Compare candle fields to historical averages. Measure magnitude in
    number of standard deviations from the mean.

    It’s simple supply and demand: a sudden increase in the number of buyers
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
    dfc = db_get(pair, freq, None)
    dfc = dfc.to_dict('record')[0]
    dfh = db_get(pair, freq, start, end=end)
    havg = dfh.describe()[1::]
    data = []

    # Price diff in past hour
    close_1h = dfc["close"] - dfh["close"].ix[-1]

    # Price vs hist. mean/std
    close_diff = dfc["close"] - havg["close"]["mean"]
    close_score = max(0, close_diff / havg["close"]["std"])
    data.append([dfc["close"], havg["close"]["mean"], close_diff, havg["close"]["std"], close_score])

    # Volume vs hist. mean/std
    vol_diff = dfc["volume"] - havg["volume"]["mean"]
    vol_score = max(0, vol_diff / havg["volume"]["std"])
    data.append([dfc["volume"], havg["volume"]["mean"], vol_diff, havg["volume"]["std"], vol_score])

    # Buy volume vs hist. mean/std
    bvol_diff = dfc["buy_vol"] - havg["buy_vol"]["mean"]
    bvol_score = max(0, bvol_diff / havg["buy_vol"]["std"])
    data.append([dfc["buy_vol"], havg["buy_vol"]["mean"], bvol_diff, havg["buy_vol"]["std"], bvol_score])

    # Buy/sell volume ratio vs hist. mean/std
    buyratio_diff = dfc["buy_ratio"] - havg["buy_ratio"]["mean"]
    buyratio_score = max(0, buyratio_diff / havg["buy_ratio"]["std"])
    data.append([dfc["buy_ratio"], havg["buy_ratio"]["mean"], buyratio_diff, havg["buy_ratio"]["std"], buyratio_score])

    # Number trades vs hist. mean/std
    trade_diff = dfc["trades"] - havg["trades"]["mean"]
    trade_score = max(0,trade_diff / havg["trades"]["std"])
    data.append([dfc["trades"], havg["trades"]["mean"], trade_diff, havg["trades"]["std"], trade_score])

    score = close_score + vol_score + bvol_score + buyratio_score + trade_score
    score = round(float(score), 2)

    df = pd.DataFrame(data,
        index=["Close", "Volume", "Buy Vol", "Buy Ratio", "Trades"],
        columns=["Candle", "Hist. Mean", "Diff", "Hist. Std", "Score"]
    ).astype(float).round(7)

    print("\nPAIR: %s" % pair)
    print("FREQ: %s" % freq)
    print("HIST: %s to %s" %(
        dfh["close_date"][0].date().strftime("%b-%d %H:%M"),
        dfh["close_date"][-1].date().strftime("%b-%d %H:%M")))
    print("CANDLE TIME: %s to %s UTC" % (
        dfc["open_date"].time().strftime("%H:%M"),
        dfc["close_date"].time().strftime("%H:%M")))
    print("SCORE: %s" % score)
    print("\n%s" % df)

    return {"havg":havg,
            "df":df,
            "score":score}
