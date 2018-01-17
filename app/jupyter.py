from datetime import datetime, date, timedelta
import pandas as pd
import pandas_datareader
import pandas_datareader.data as web



# GLOBALS
SOURCE = 'quandl'
PLT_USER = 'SeanEstey'
PLT_API = 'i1TT0V3ckOQXBPguMCSB'



# Valid gtrends:
# 'now': ['1-d', '7-d'], 'today': ['1-m', '2-m', '3-m', '1-y', '2-y', '3-y'], 'all': n/a
today_d = date.today()
today_dt = datetime(today_d.year, today_d.month, today_d.day)
timeframes = {
    '7-d': {
        'start': today_dt - timedelta(days=7),
        'end': today_dt,
        'trend':'now 7-d',
        'frequency':'H'
    },
    '1-m': {
        'start': today_dt - timedelta(days=31),
        'end': today_dt,
        'trend':'today 1-m',
        'frequency':'D'
    },
    '3-m': {
        'start': today_dt - timedelta(days=93),
        'end': today_dt,
        'trend':'today 3-m',
        'frequency':'D'
    },
    '1-y': {
        'start': today_dt - timedelta(days=365),
        'end': today_dt,
        'trend':'today 1-y',
        'frequency':'W' # week
    }
}

# SELECT DATA
ASSET = assets['bitcoin cash']
RANGE = timeframes['7-d']
FREQUENCY = 'D'

##### PRICE DATA #####
df = web.DataReader(
    ASSET['pair_symbol'],
    SOURCE,
    RANGE['start'],
    RANGE['end'])
# Ascending order
df = df.iloc[::-1]
# Fill missing dates

df = df.reindex(
    pd.date_range(df.index.min(), df.index.max(), freq='D')
)
df['Price Diff'] = df['Mid'].diff(periods=1)

##### TREND DATA #####
pytrends = TrendReq(
    hl='en-US',
    tz=360)
pytrends.build_payload(
    [ASSET['name']],
    timeframe=RANGE['trend'],
    cat=0,
    geo='',
    gprop='')
df2 = pytrends.interest_over_time()
df2.rename(
    columns={ASSET['name']:'Trend'},
    inplace=True)
df2.drop(df2.columns[[1]], axis=1, inplace=True)
_df2 = df2.copy()

# TEMP. REMOVE ME
df2.index = df2.index.normalize()
df2['Trend'] = df2['Trend'].resample('D').mean()
df2 = df2.drop_duplicates()
#df2 = df2.reindex(
#    pd.date_range(df2.index.min(), df2.index.max(), freq=FREQUENCY)
#)

# Calc indicators
df2['Trend Diff'] = df2['Trend'].diff(periods=1)
df2['Trend MA'] = df2['Trend'].rolling(10).mean()
df2.rename(columns={'date':'Date'},inplace=True)

# Merge datasets by date index
df3 = df.join(df2)
# Normalize
df3= ( (df3-df3.min())/(df3.max()-df3.min()) ) * 100
