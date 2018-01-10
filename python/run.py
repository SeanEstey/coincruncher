"""Grabs prices from Coinmarketcap API, writes them to Default.js file in Numi
extension directory.
Docs: https://coinmarketcap.com/api/
"""
import itertools, requests, json, os, re, sys, time
from money import Money
from decimal import Decimal
from pprint import pprint
from datetime import datetime
from time import sleep

WCI_API_KEY = "B8BDV74aQIoF5rQYgZNdQ8VBfdgPN0";
WCI_MARKETS_URL = "https://www.worldcoinindex.com/apiservice/getmarkets";
wci_uri = WCI_MARKETS_URL + "?key=" + WCI_API_KEY + "&fiat=cad";
EXT_PATH = "%s/Library/Application Support/com.dmitrynikolaev.numi/extensions" % os.path.expanduser("~")
COINCAP_BASE_URL = "https://api.coinmarketcap.com/v1"
COINCAP_TICKER_URL = "https://api.coinmarketcap.com/v1/ticker/?convert=CAD&limit=200"
spinner = itertools.cycle(['-', '/', '|', '\\'])
frequency = 30
msg = ''
currency = "CAD"
ticker_data = []
watchlist = []
portfolio = []

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

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
def export_numi(data):
    # Set numi "marketcap" global var
    buf = ''
    buf += "\nnumi.setVariable(\"marketcap\", { \"double\":%s, \"unitId\":\"%s\"});" %(
        markets['total_market_cap_cad'], currency)

    buf += "%s=%s;  " % (coin['js_name'], coin['price'])

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
def colorize(val):

    if isinstance(val, Money):
        return "%s%s%s%s" %(
            bcolors.FAIL if val.amount < 0 else bcolors.OKGREEN,
            "+" if val.amount > 0 else "",
            val.format('en_US', '###,###'),
            bcolors.ENDC)
    elif type(val) == float:
        return "%s%s%s%s" %(
            bcolors.FAIL if val < 0 else bcolors.OKGREEN,
            "+" if val > 0 else "",
            str(round(val,1)) + '%',
            bcolors.ENDC)

#----------------------------------------------------------------------
def justify(col, width):
    rmv_esc = re.compile(r'\x1b[^m]*m')
    str_val = rmv_esc.sub('',col)
    escapes = re.findall(rmv_esc, col)
    widened = str_val.ljust(width)
    escapes.insert(1, widened)
    return "".join(escapes)

#----------------------------------------------------------------------
def get_width(val):
    rmv_esc = re.compile(r'\x1b[^m]*m')
    fixed_len = rmv_esc.sub('', str(val))
    return len(fixed_len)

#----------------------------------------------------------------------
def get_wci_markets():
    r = requests.get(wci_uri)
    r = json.loads(r.text)
    pprint(r)

#----------------------------------------------------------------------
def show_markets():
    try:
        r = requests.get(COINCAP_BASE_URL + "/global?convert=%s" % currency)
        markets = json.loads(r.text)
    except Exception as e:
        return False

    row = [
        humanize(Money(markets['total_market_cap_cad']+0.1, 'CAD')),
        humanize(Money(markets['total_24h_volume_cad']+0.1, 'CAD')),
        str(round(markets['bitcoin_percentage_of_market_cap'],2))+'%',
        str(markets['active_currencies'])
    ]
    header = ['Market Cap', '24h Volume', 'BTC Dominance', 'Currencies']
    col_widths = [len(n) for n in header]
    col_widths = [max(col_widths[n], get_width(row[n])) for n in range(0,len(row))]

    print("\n    %s\n\n    %sGlobal (CAD)%s" % (
        datetime.now().strftime("%h %d %H:%M:%S"), bcolors.BOLD, bcolors.ENDC))
    print("    " + "".join(justify(header[n], col_widths[n]+2) for n in range(0,len(header))))
    print("    " + "".join(justify(row[n], col_widths[n]+2) for n in range(0,len(row))))
    print("")

#----------------------------------------------------------------------
def show_watchlist():
    global watchlist, ticker_data

    try:
        r = requests.get(COINCAP_TICKER_URL)
        ticker_data = json.loads(r.text)
    except Exception as e:
        print("Request error: %s" % str(e))

    try:
        r = requests.get("%s/ticker/%s/?convert=CAD" %(COINCAP_BASE_URL, 'dotcoin'))
        ticker_data.append(json.loads(r.text)[0])
    except Exception as e:
        print("Request error for dotcoin")

    rows = []
    for watch in watchlist:
        for coin in ticker_data:
            if coin['id'] != watch['name']:
                continue

            rows.append([
                coin['rank'],
                coin['symbol'],
                Money(float(coin['price_cad']), 'CAD').format('en_US', '$###,###'),
                colorize(float(coin["percent_change_1h"])),
                colorize(float(coin["percent_change_24h"])),
                colorize(float(coin["percent_change_7d"])),
                humanize(Money(float(coin['market_cap_cad']), 'CAD'))
            ])

    header = ["Rank", "Symbol", "Price", "1h", "24h", "7d", "Mcap"]
    col_widths = [len(n) for n in header]
    for row in rows:
        col_widths = [max(col_widths[n], get_width(row[n])) for n in range(0,len(row))]

    print("    %sWatching (CAD)%s" %(bcolors.BOLD, bcolors.ENDC))
    print("    " +  "".join(justify(
        header[n], col_widths[n]+2) for n in range(0,len(header)))) 
    for row in sorted(rows, key=lambda x: int(x[0])):
        print("    " + "".join(justify(
            row[n], col_widths[n]+2) for n in range(0,len(row))))

#----------------------------------------------------------------------
def show_portfolio():
    global watchlist, portfolio
    total = 0.0
    rows = []
    profit = Money(0.0, 'CAD')
    # Build table data
    for hold in portfolio:
        for coin in ticker_data:
            if coin['symbol'] != hold['symbol']:
                continue

            total += hold['amount'] * float(coin['price_cad'])

            rows.append([
                coin['rank'],
                coin['symbol'],
                Money(float(coin['price_cad']), 'CAD'),
                hold['amount'],
                Money(round(hold['amount'] * float(coin['price_cad']),2),'CAD'), # Value
                "", # Portion %
                colorize(float(coin["percent_change_1h"])),
                colorize(float(coin["percent_change_24h"])),
                colorize(float(coin["percent_change_7d"]))
            ])

            profit += Decimal(float(coin['percent_change_24h'])/100) * rows[-1][4]

    rows = sorted(rows, key=lambda x: int(x[4]))[::-1]
    total = Money(total, 'CAD')
    header = ['Rank', 'Symbol', 'Price', 'Amount', 'Value', 'Portion', '1h', '24h', '7d']
    col_widths = [len(n) for n in header]

    for row in rows:
        row[5] = str(round((row[4] / total) * 100, 2)) + '%'
        row[2] = row[2].format('en_US', '$###,###')
        row[4] = row[4].format('en_US', '$###,###')

        col_widths = [max(col_widths[n], get_width(row[n])) for n in range(0,len(row))]

    print("\n    %sPortfolio (CAD)%s" % (bcolors.BOLD, bcolors.ENDC))
    print("    " + "".join(justify(
        header[n], col_widths[n]+2) for n in range(0,len(header))))
    for row in rows: #sorted(rows, key=lambda x: int(x[0])):
        print("    " + "".join(justify(
            str(row[n]), col_widths[n]+2) for n in range(0,len(row))))
    print("") #    ---------------------------------------------------------")
    print("    %s$%s%s (%s%s%s)" % (
        bcolors.BOLD, total.format('en_US', '###,###'), bcolors.ENDC,
        bcolors.BOLD, colorize(profit), bcolors.ENDC))
    #print("    Profit (24h): %s" % colorize(profit))

#----------------------------------------------------------------------
if __name__ == "__main__":

    user_data = json.load(open('data.json'))
    watchlist = user_data['watchlist']
    portfolio = user_data['portfolio']

    print("Updating prices in CAD every %ss...\n" % frequency)

    while True:
        show_markets()
        show_watchlist()
        show_portfolio()
        update_spinner()
