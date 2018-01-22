import logging
from datetime import datetime, timedelta
import pandas as pd
from app import db
log = logging.getLogger(__name__)

#------------------------------------------------------------------------------
def mktcap_resample(freq):
    """@freq: '1H', '1D', '7D'
    """
    if freq == '1H':
        from_dt = datetime.now() - timedelta(hours=1)
    elif freq == '1D':
        from_dt = datetime.now() - timedelta(hours=24)
    else:
        print("unknown freq %s" % freq)

    mktcaps = db.coinmktcap_markets.find(
        {'datetime':{'$gte':from_dt}},
        {'_id':0,'n_assets':0,'n_currencies':0,'n_markets':0,'pct_mktcap_btc':0})

    df = pd.DataFrame(list(mktcaps))
    df.index = df['datetime']
    del df['datetime']
    df = df.resample(freq).mean()
    return df

#------------------------------------------------------------------------------
def mktcap_diff(freq):
    """@freq: '1H', '1D', '7D'
    """
    caps = list(mktcap_resample(freq)['mktcap_cad'])
    diff = round(((caps[-1] - caps[-2]) / caps[-2]) * 100, 2)
    return diff
