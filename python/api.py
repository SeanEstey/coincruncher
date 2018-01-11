import requests, json
from pprint import pprint


COINCAP_BASE_URL = "https://api.coinmarketcap.com/v1"
COINCAP_TICKER_URL = "https://api.coinmarketcap.com/v1/ticker/?convert=CAD&limit=200"
WCI_API_KEY = "B8BDV74aQIoF5rQYgZNdQ8VBfdgPN0";
WCI_MARKETS_URL = "https://www.worldcoinindex.com/apiservice/getmarkets";
wci_uri = WCI_MARKETS_URL + "?key=" + WCI_API_KEY + "&fiat=cad";

#----------------------------------------------------------------------
def get_wci_markets():
    r = requests.get(wci_uri)
    r = json.loads(r.text)
    pprint(r)

#----------------------------------------------------------------------
def get_markets(currency):
    data=None
    try:
        r = requests.get(COINCAP_BASE_URL + "/global?convert=%s" % currency)
        data = json.loads(r.text)
    except Exception as e:
        return False
    else:
        return data

#----------------------------------------------------------------------
def get_ticker():
    data=None
    try:
        r = requests.get(COINCAP_TICKER_URL)
        data = json.loads(r.text)
    except Exception as e:
        print("Request error: %s" % str(e))

    try:
        r = requests.get("%s/ticker/%s/?convert=CAD" %(COINCAP_BASE_URL, 'dotcoin'))
        data.append(json.loads(r.text)[0])
    except Exception as e:
        print("Request error for dotcoin")
    else:
        return data
