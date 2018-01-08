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

WCI_API_KEY = "B8BDV74aQIoF5rQYgZNdQ8VBfdgPN0";
WCI_MARKETS_URL = "https://www.worldcoinindex.com/apiservice/getmarkets";
wci_uri = WCI_MARKETS_URL + "?key=" + WCI_API_KEY + "&fiat=cad";

def get_wci_markets():

    r = requests.get(wci_uri)
    r = json.loads(r.text)
    pprint(r)

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
EXT_PATH = "%s/Library/Application Support/com.dmitrynikolaev.numi/extensions" % os.path.expanduser("~")
COINCAP_BASE_URL = "https://api.coinmarketcap.com/v1"
COINCAP_TICKER_URL = "https://api.coinmarketcap.com/v1/ticker/?convert=CAD&limit=200"

spinner = itertools.cycle(['-', '/', '|', '\\'])
frequency = 30
buf = ""
msg = ''
currency = "CAD"
coins = [
    {"name":"bitcoin", "symbol":"BTC", "js_name":"BTC_CAD"},
    {"name":"ethereum", "symbol": "ETH", "js_name":"ETH_CAD"},
    {"name":"raiblocks", "symbol":"XRB", "js_name":"XRB_CAD"},
    {"name":"siacoin", "symbol": "SC", "js_name":"SC_CAD"},
    {"name":"iota", "symbol": "IOT", "js_name":"IOT_CAD"},
    {"name":"cardano", "symbol": "ADA", "js_name":"ADA_CAD"},
    {"name":"litecoin", "symbol": "LTC", "js_name":"LTC_CAD"},
    {"name":"spankchain", "symbol": "SPANK", "js_name":"SPANK_CAD"},
    {"name":"omisego", "symbol": "OMG", "js_name":"OMG_CAD"},
    {"name":"burst", "symbol": "BURST", "js_name":"BURST_CAD"},
    {"name":"deepbrain-chain", "symbol": "DBC", "js_name":"DBC_CAD"},
    {"name":"walton", "symbol": "WTC", "js_name":"WTC_CAD"},
    {"name":"neo", "symbol": "NEO", "js_name":"NEO_CAD"},
    {"name":"request-network", "symbol": "REQ", "js_name":"REQ_CAD"},
    {"name":"tron", "symbol":"TRX", "js_name":"TRX_CAD"},
    {"name":"vechain", "symbol": "VEN", "js_name":"VEN_CAD"},
    {"name":"substratum", "symbol":"SUB", "js_name":"SUB_CAD"},
    {"name":"airswap", "symbol": "AST", "js_name":"AST_CAD"},
    {"name":"nav-coin", "symbol": "NAV", "js_name":"NAV_CAD"},
    {"name":"eos", "symbol":"EOS", "js_name":"EOS_CAD"},
    {"name":"bitcoin-cash", "symbol":"BCH", "js_name":"BCH_CAD"},
    {"name":"binance-coin", "symbol":"BNB", "js_name":"BNB_CAD"},
    {"name":"storj", "symbol": "STORJ", "js_name":"STORJ_CAD"},
    #{"name":"dotcoin", "symbol": "DOT", "js_name":"DOT_CAD"},
]

portfolio = [
    {"symbol":"XRB", "amount":6500.0},
    {"symbol":"ETH", "amount":124.0},
    {"symbol":"IOT", "amount":2685.0},
    {"symbol":"SC", "amount":124609.0},
    {"symbol":"ADA", "amount":9712.0},
    {"symbol":"SPANK", "amount":13927.0},
    {"symbol":"OMG", "amount":154.78},
    {"symbol":"BURST", "amount":29498.22},
    {"symbol":"DBC", "amount":8686.96},
    {"symbol":"NEO", "amount":25.30},
    {"symbol":"REQ", "amount":2529.44},
    {"symbol":"VEN", "amount":481.00},
    {"symbol":"SUB", "amount":828.00},
    {"symbol":"TRX", "amount":13791.79},
    {"symbol":"AST", "amount":1485.00},
    {"symbol":"NAV", "amount":174.24},
    {"symbol":"WTC", "amount":100.00},
    {"symbol":"EOS", "amount":91.90},
    #{"symbol":"SAFEX", "amount":17707.17},
    {"symbol":"BNB", "amount":17.69},
    {"symbol":"STORJ", "amount":115.88},
    #{"symbol":"DOT", "amount":8072.23475754},
    {"symbol":"BTC", "amount":0.01}
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

    global coins
    buf = ""

    try:
        r = requests.get(COINCAP_TICKER_URL)
        data = json.loads(r.text)
    except Exception as e:
        print("Request error: %s" % str(e))
        #pass

    # Get coin prices
    for coin in coins:
        for result in data:
            if result['id'] != coin['name']:
                continue

            coin['price'] = round(float(result['price_cad']),2)
            coin['mcap'] = round(float(result['market_cap_cad']),2) + 0.1
            coin['rank'] = result['rank']
            coin['percent_change_1h'] = float(result['percent_change_1h'])
            coin['percent_change_24h'] = float(result['percent_change_24h'])
            coin['percent_change_7d'] = float(result['percent_change_7d'])

            buf += "%s=%s;  " % (coin['js_name'], coin['price'])

    # Set numi "marketcap" global var
    r = requests.get(COINCAP_BASE_URL + "/global?convert=%s" % currency)
    markets = json.loads(r.text)
    buf += "\nnumi.setVariable(\"marketcap\", { \"double\":%s, \"unitId\":\"%s\"});" %(
        markets['total_market_cap_cad'], currency)

    # Write data to extension
    try:
        file = open("%s/data.js" % EXT_PATH,"w")
        file.write(buf)
        file.close()
    except Exception as e:
        print_markets(markets)
        pass
    else:
        print_markets(markets)

#----------------------------------------------------------------------
def humanize(money):

    mag = money.amount.adjusted()
    title = ''
    amount = None

    if mag >= 3 and mag <= 5:
        amount = round(money.amount/pow(10,3),2)
        title = 'thousand'
    elif mag >=6 and mag <=8:
        amount = round(money.amount/pow(10,6),2)
        title = 'M'
    elif mag >=9 and mag <=11:
        amount = round(money.amount/pow(10,9),2)
        title = 'B'
    elif mag >= 12 and mag <= 14:
        amount = round(money.amount/pow(10,12),2)
        title = 'T'

    return '$%s%s' %(amount, title)

#----------------------------------------------------------------------
def colorize(perc):
    return "%s%s%s" %(
        bcolors.FAIL if perc < 0 else bcolors.OKGREEN,
        str(round(perc,1)) + '%',
        bcolors.ENDC)

#----------------------------------------------------------------------
def print_markets(markets):

    print("%s\n  Markets\n    Mcap=%s, 24Vol=%s " %(
        datetime.now().strftime("%h %d %H:%M:%S"),
        humanize(Money(markets['total_market_cap_cad']+0.1, 'CAD')),
        humanize(Money(markets['total_24h_volume_cad']+0.1, 'CAD'))))

    print("  Watching (CAD)")
    sorted_coins = sorted(coins, key=lambda k: k['mcap'])

    header = ["Rank", "Symbol", "Price", "1h", "24h", "7d", "Mcap"]
    col_widths = [len(col) for col in header]
    rows = [header]

    for coin in sorted_coins[::-1]:
        row =[
            "#%s" % coin['rank'],
            coin['symbol'],
            Money(amount=coin['price'], currency='CAD').format('en_US', '$###,###'),
            str(round(coin["percent_change_1h"],1))+"%",
            str(round(coin["percent_change_24h"],1))+"%",
            str(round(coin["percent_change_7d"],1))+"%",
            humanize(Money(coin['mcap'], 'CAD'))
        ]

        # TODO: remove esc characters then calculate col_width
        #import re
        #re.compile(r'\x1b[^m]*m')
        for idx in range(0, len(row)):
            col_widths[idx] = max(len(row[idx]), col_widths[idx])

        rows.append(row)

    for idx in range(0, len(rows)):
        row = rows[idx]

        if idx == 0:
            print("    %s%s%s" %(
                bcolors.BOLD, "".join(row[n].ljust(col_widths[n]+2) for n in range(0,len(row))),bcolors.ENDC))
            continue

        row[0] = row[0].ljust(col_widths[0]+2)
        row[1] = row[1].ljust(col_widths[1]+2)
        row[2] = row[2].ljust(col_widths[2]+2)
        row[3] =  "%s%s" %(bcolors.FAIL if float(row[3][:-1]) < 0 else bcolors.OKGREEN, row[3].ljust(col_widths[3]+2))
        row[4] =  "%s%s" %(bcolors.FAIL if float(row[4][:-1]) < 0 else bcolors.OKGREEN, row[4].ljust(col_widths[4]+2))
        row[5] =  "%s%s" %(bcolors.FAIL if float(row[5][:-1]) < 0 else bcolors.OKGREEN, row[5].ljust(col_widths[5]+2))
        row[6] = bcolors.ENDC + row[6].ljust(col_widths[6]+2)

        print("    " + "".join(row)) # + bcolors.ENDC)

#----------------------------------------------------------------------
def print_portfolio():

    import re
    re.compile(r'\x1b[^m]*m')

    total = Money(0.0, 'CAD')
    print("  Portfolio (CAD)")

    header = ['Symbol', 'Amount', 'Price', 'Value', 'Portion']
    data = [header]

    # Build table data
    for hold in portfolio:
        for coin in coins:
            if coin['symbol'] != hold['symbol']:
                continue

            row =[
                coin['symbol'],
                hold['amount'],
                Money(amount=coin['price'], currency='CAD'), #.format('en_US', '$###,###'),
                Money(round(hold['amount'] * coin['price'],2),'CAD'),
                "%##.##"
            ]
            total += hold['value']
            data.append(row)

    # Get column widths
    widths=[0,0,0,0,0]
    for idx in range(0, len(portfolio)):
        widths[idx] = max(len(str(portfolio[idx])), widths[idx])

    # Print portfolio table
    for datarow in data:
        # Portfolio %
        fmt_row = []
        datarow[4] = round((datarow[3] / datarow[2])*100,2)

        row = [
            coin['symbol'],
            hold['amount'],

        ]

        print("    %s: $%s (%s)" %(
            hold['symbol'],
            hold['value'].format('en_US', '###,###', currency_digits=True),
            str(hold['perc']) + '%'))

    print("    ---------------")
    print("    Total: %s$%s%s" %(
        bcolors.OKGREEN,
        total.format('en_US', '###,###', currency_digits=True),
        bcolors.ENDC))
#----------------------------------------------------------------------
if __name__ == "__main__":
    print("Updating prices in CAD every %ss...\n" % frequency)
    while True:
        get_data()
        #print_portfolio()
        update_spinner()
