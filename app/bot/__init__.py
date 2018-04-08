import pandas as pd
from binance.client import Client
def pct_diff(a,b): return ((b-a)/a)*100
dfc = pd.DataFrame()
client = Client("", "")
from . import candles, candles, macd, printer, scanner, signals, strategy, trade
