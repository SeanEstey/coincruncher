# app.bnc
from docs.rules import RULES
from docs.data import BINANCE

def siglog(msg): log.log(100, msg)
def pct_diff(a,b): return ((b-a)/a)*100

rules = RULES['1m'] # FIXME
start = now()
dfc = pd.DataFrame()
n_cycles = 0
mkt_move, freq, freq_str = None, None, None
pairs = BINANCE['PAIRS']
ema_span = rules['EMA']['SPAN']
z_thresh = rules['Z-SCORE']['THRESH']
client = None
