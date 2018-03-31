import pandas as pd
from binance.client import Client
from docs.rules import RULES
from docs.data import BINANCE

def pct_diff(a,b): return ((b-a)/a)*100

# VARS
pairs = BINANCE['PAIRS']
# FIXME
rules = RULES['1m']
dfc = pd.DataFrame()
client = Client("", "")






















#------------------------------------------------------------------------------
def _unfinished():
    # *********************************************************************
    # Calculate Z-Scores, store in dataframe/mongodb
    # ops=[]
    # for pair in pairs:
    #    candle = candles.newest(pair, freq_str, df=dfc)
    #    scores = signals.z_score(
    #        dfc.loc[pair,freq], candle, mkt_ma=mkt_ma)
    #    name = 'ZSCORE_' + freq_str.upper()
    #   dfc[name].loc[pair,freq][-1] = scores['CLOSE']['ZSCORE'].round(3)
    #   ops.append(UpdateOne({"open_time":candle["OPEN_TIME"],
    #       "pair":candle["PAIR"], "freq":candle["FREQ"]},
    #       {'$set': {name: scores['CLOSE']['ZSCORE']}}
    #   ))
    #   db.candles.bulk_write(ops)
    #
    #   if c2['OPEN_TIME'] < c1['OPEN_TIME']:
    #       return False
    # *********************************************************************

    # ********************************************************************
    # A. Profit loss
    # if c2['CLOSE'] < c1['CLOSE']:
    #    if 'Resistance' not in holding['buy']['details']:
    #        return sell(holding, c2, scores)
    #    margin = signals.adjust_support_margin(freq_str, mkt_ma)
    #    if (c2['CLOSE'] * margin) < c1['CLOSE']:
    #        return sell(holding, c2, scores)
    # B. Maximize profit, make sure price still rising.
    # p_max = df.loc[slice(c1['OPEN_TIME'], df.iloc[-2].name)]['CLOSE'].max()
    # elif not np.isnan(p_max) and candle['CLOSE'] < p_max:
    #   return sell(holding, c2, scores)
    # ********************************************************************

    # ********************************************************************
    # Open Trades (If Sold at Present Value)
    # pct_change_hold = []
    # active = list(db.trades.find({'status':'open'}))
    # for hold in active:
    #    candle = candles.newest(hold['pair'], freq_str, df=dfc)
    #    pct_change_hold.append(pct_diff(hold['buy']['candle']['CLOSE'], candle['CLOSE']))
    #
    # if len(pct_change_hold) > 0:
    #     pct_change_hold = sum(pct_change_hold)/len(pct_change_hold)
    # else:
    #     pct_change_hold = 0.0
    #
    # siglog("Holdings: {} Open, {:+.2f}% Mean Value".format(len(active), pct_change_hold))
    # siglog('-'*80)
    # ********************************************************************
    pass

#-----------------------------------------------------------------------------
def print_tickers():
    # *********************************************************************
    # TODO: Create another trading log for detailed ticker tarding signals.
    # Primary siglog will be mostly for active trading/holdings.
    # *********************************************************************
    pass
