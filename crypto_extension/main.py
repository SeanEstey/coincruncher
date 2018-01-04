"""Grabs prices from Coinmarketcap API, writes them to Default.js file in Numi
extension directory.
Docs: https://coinmarketcap.com/api/
"""
import requests, json, os, time
from datetime import datetime

EXT_PATH = "%s/Library/Application Support/com.dmitrynikolaev.numi/extensions" % os.path.expanduser("~")
COINCAP_BASE_URL = "https://api.coinmarketcap.com/v1"
#WCI_API_KEY = "B8BDV74aQIoF5rQYgZNdQ8VBfdgPN0";
#WCI_MARKETS_URL = "https://www.worldcoinindex.com/apiservice/getmarkets";
#wci_uri = WCI_MARKETS_URL + "?key=" + WCI_API_KEY + "&fiat=cad";
currency = "CAD"
coins = [
    {"name":"raiblocks", "js_name":"XRB_CAD"},
    {"name":"ethereum","js_name":"ETH_CAD"},
    {"name":"bitcoin", "js_name":"BTC_CAD"}
]

#----------------------------------------------------------------------
def get_data():
    buf = ""

    # Get coin prices
    for coin in coins:
        r = requests.get(COINCAP_BASE_URL + "/ticker/%s/?convert=%s" %(coin['name'], currency))
        data = json.loads(r.text)
        price = data[0]['price_cad']
        buf += "%s=%s;  " % (coin['js_name'], round(float(price),2))

    # Set numi "marketcap" global var
    r = requests.get(COINCAP_BASE_URL + "/global?convert=%s" % currency)
    data = json.loads(r.text)
    buf += "\nnumi.setVariable(\"marketcap\", { \"double\":%s, \"unitId\":\"%s\"});" %(data['total_market_cap_cad'], currency)

    # Write data to extension
    file = open("%s/Default.js" % EXT_PATH,"w")
    now = datetime.now()
    print("%s: %s" % (now.ctime(), buf))
    file.write(buf)
    file.close()

#----------------------------------------------------------------------
if __name__ == "__main__":
    print("Fetching prices every 5s...")
    while True:
        get_data()
        time.sleep(5)
