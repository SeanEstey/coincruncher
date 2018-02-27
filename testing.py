import logging, time
from importlib import reload
from datetime import timedelta as tdelta
import pandas as pd
from app import get_db, set_db
from app.timer import Timer

# Config
log = logging.getLogger("testing")
pd.set_option("display.max_columns", 25)
pd.set_option("display.width", 2000)
hosts = ["localhost", "45.79.176.125"]
set_db(hosts[0])
db = get_db()
t1 = Timer()

#------------------------------------------------------------------------------
def sigstrfrom(pair, n_days):
    dfh = db_get(pair, now()-tdelta(days=n_days), now()-tdelta(minutes=5))
    dfc = db_get(pair, now()-tdelta(minutes=10))
    print(dfc)
    candle = dfc.pct_change() #.to_dict('records')[1]
    print(candle)
    return signals.sigstr(candle, dfh)

#------------------------------------------------------------------------------
from app import signals
from pprint import pprint
from app.candles import db_get, api_get, to_df
from app.utils import utc_datetime as now

sigstrfrom("NANOBTC", 7)
sigstrfrom("NANOBTC", 3)





#------------------------------------------------------------------------------
def test1():
    df_new = df_hst.tail(2)
    df_hst = df_hst.head(len(df_hst)-2)
    #df_hst = df_hst.pct_change()
    #df_new = df_new.pct_change()
    candle = df_new.ix[0].to_dict()
    candle["date"] = df_new.index[0].to_pydatetime()
    candle["pair"] = pair
    signals.sigstr(candle, df_hst)
