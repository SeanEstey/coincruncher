"""Grabs prices from Coinmarketcap API, writes them to Default.js file in Numi
extension directory.
Docs: https://coinmarketcap.com/api/
"""
import itertools, requests, json, os, sys, time
from money import Money
from decimal import Decimal
from pprint import pprint
from datetime import datetime
from time import sleep



EXT_PATH = "%s/Library/Application Support/com.dmitrynikolaev.numi/extensions" % os.path.expanduser("~")
COINCAP_BASE_URL = "https://api.coinmarketcap.com/v1"
#WCI_API_KEY = "B8BDV74aQIoF5rQYgZNdQ8VBfdgPN0";
#WCI_MARKETS_URL = "https://www.worldcoinindex.com/apiservice/getmarkets";
#wci_uri = WCI_MARKETS_URL + "?key=" + WCI_API_KEY + "&fiat=cad";

spinner = itertools.cycle(['-', '/', '|', '\\'])
frequency = 30
buf = ""
msg = ''
currency = "CAD"
coins = [
    {"name":"bitcoin", "symbol":"BTC", "js_name":"BTC_CAD", "price":None},
    {"name":"ethereum","symbol": "ETH", "js_name":"ETH_CAD", "price":None},
    {"name":"raiblocks", "symbol":"XRB", "js_name":"XRB_CAD", "price":None}
]

#----------------------------------------------------------------------
def update_spinner():
    i=0
    while i < frequency:
        msg = '%s' % next(spinner)
        sys.stdout.write(msg)
        sys.stdout.flush()
        sys.stdout.write('\b'*len(msg))
        i+=1
        sleep(1)

#----------------------------------------------------------------------
def get_data():
    buf = ""

    # Get coin prices
    for coin in coins:
        r = requests.get(COINCAP_BASE_URL + "/ticker/%s/?convert=%s" %(coin['name'], currency))
        data = json.loads(r.text)
        coin['price'] = round(float(data[0]['price_cad']),2)
        coin['mcap'] = round(float(data[0]['market_cap_cad']),2)
        coin['rank'] = data[0]['rank']
        buf += "%s=%s;  " % (coin['js_name'], coin['price'])

    # Set numi "marketcap" global var
    r = requests.get(COINCAP_BASE_URL + "/global?convert=%s" % currency)
    markets = json.loads(r.text)
    buf += "\nnumi.setVariable(\"marketcap\", { \"double\":%s, \"unitId\":\"%s\"});" %(
        markets['total_market_cap_cad'], currency)

    # Write data to extension
    try:
        file = open("%s/Default.js" % EXT_PATH,"w")
        file.write(buf)
        file.close()
    except Exception as e:
        print_data(markets)
        pass
    else:
        now = datetime.now()
        print("%s: %s" % (now.ctime(), buf))

#----------------------------------------------------------------------
def humanize(money):

    mag = money.amount.adjusted()
    title = ''
    amount = None

    if mag >= 3 and mag <= 5:
        amount = money.amount.shift(-3).quantize(Decimal('1.0'))
        title = 'thousand'
    elif mag >=6 and mag <=8:
        amount = money.amount.shift(-6).quantize(Decimal('1.0'))
        title = 'million'
    elif mag >=9 and mag <=11:
        amount = money.amount.shift(-9).quantize(Decimal('1.0'))
        title = 'billion'
    elif mag >= 12 and msg <= 14:
        amount = money.amount.shift(-12).quantize(Decimal('1.0'))
        title = 'trillion'

    return '$%s %s' %(amount, title)

#----------------------------------------------------------------------
def print_data(markets):

    print("%s\n\tMcap=%s, 24Vol=%s " %(
        datetime.now().strftime("%h %d %H:%M:%S"),
        humanize(Money(markets['total_market_cap_cad']+0.1, 'CAD')),
        humanize(Money(markets['total_24h_volume_cad']+0.1, 'CAD'))))

    line = ''
    for coin in coins:
        m = Money(amount=coin['price'], currency='CAD')
        #line += "%s=%s, " %(coin['symbol'], m.format('en_US', '$###,###', currency_digits=True))
        print("\t%s: Price=%s, Mcap=%s, Rank=%s" %(
            coin['symbol'],
            m.format('en_US', '$###,###', currency_digits=True),
            humanize(Money(coin['mcap']+0.1, 'CAD')),
            coin['rank']
        ))

    print('\t%s' % line)

#----------------------------------------------------------------------
if __name__ == "__main__":
    print("Updating prices in CAD every %ss..." % frequency)
    while True:
        get_data()
        update_spinner()
