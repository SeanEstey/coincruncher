import logging
import pandas as pd
from app import db
log = logging.getLogger(__name__)


def mktcap():
     mktcaps = db.coinmktcap_markets.find()#.sort('_id',-1)
     df = pd.DataFrame(list(mktcaps))
     del df['_id']
     # TODO: Filter results from past 24 hrs only
     df.index = df['datetime']
     df['hour'] = [ts.hour for ts in df.index]
     df.groupby('hour').mean()

     mkt_hourly = list(df['mktcap_cad'])

     log.info("mkt_hourly=%s", mkt_hourly)

     mkt_1h_diff = (mkt_hourly[-1] - mkt_hourly[-2])/mkt_hourly[-2] * 100
     mkt_1h_diff = round(mkt_1h_diff, 2)

     log.info("mkt_1h=%s", mkt_1h_diff)

     return mkt_1h_diff
