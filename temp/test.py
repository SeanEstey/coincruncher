# https://github.com/Crypto-toolbox/bitex
import time
from json import loads
from pprint import pprint
from bitex import QuadrigaCX, Bitfinex, Binance
#from bitex.interface import Binance, Bitfinex, Bittrex, Bitstamp, CCEX, CoinCheck
#from bitex.interface import Cryptopia, HitBTC, Kraken, OKCoin, Poloniex, QuadrigaCX
#from bitex.interface import TheRockTrading, Vaultoro

def get_binance_tickers():
    binance = Binance()
    #print(binance._get_supported_pairs())
    ticker = binance.ticker('BTCUSDT')
    print(json.loads(ticker.text))

def get_quadcx_tickers():
    from bitex import QuadrigaCX
    quadcx = QuadrigaCX()
    pprint(quadcx._get_supported_pairs())
    pprint(loads(quadcx.ticker('btc_cad').text))

def get_quadcx_wss_tickers():
    quadcx = QuadrigaCX()
    #pprint(quadcx._get_supported_pairs())
    #pprint(loads(quadcx.ticker('btc_cad').text))

def get_bitfinex_tickers():
    bitfinex = Bitfinex()
    print(bitfinex._get_supported_pairs())


get_quadcx_tickers()
