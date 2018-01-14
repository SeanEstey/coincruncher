# Display formatted text to stdout in table form
import itertools, logging, re, sys, time
from datetime import datetime
from time import sleep
from money import Money
from decimal import Decimal
from config import *
from app import db
log = logging.getLogger(__name__)

spinner = itertools.cycle(['-', '/', '|', '\\'])
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
def show_markets():
    markets = list(db.markets.find().limit(1).sort('_id',-1))[0]

    row = [
        humanize(Money(markets['total_market_cap_%s' % CURRENCY], CURRENCY.upper())),
        humanize(Money(markets['total_24h_volume_%s' % CURRENCY], CURRENCY.upper())),
        str(round(markets['bitcoin_percentage_of_market_cap'],2))+'%',
        str(markets['active_currencies'])
    ]
    header = ['Market Cap', '24h Volume', 'BTC Dominance', 'Currencies']
    col_widths = [len(n) for n in header]
    col_widths = [max(col_widths[n], get_width(row[n])) for n in range(0,len(row))]

    print("\n    %s\n\n    %sGlobal (%s)%s" % (
        datetime.now().strftime("%h %d %H:%M:%S"), bcolors.BOLD, CURRENCY, bcolors.ENDC))
    print("    " + "".join(justify(header[n], col_widths[n]+2) for n in range(0,len(header))))
    print("    " + "".join(justify(row[n], col_widths[n]+2) for n in range(0,len(row))))
    print("")

#----------------------------------------------------------------------
def show_watchlist(watchlist):
    rows = []
    for watch in watchlist:
        for coin in db.tickers.find():
            if coin['id'] != watch['id']:
                continue

            rows.append([
                coin['rank'],
                coin['symbol'],
                Money(float(coin['price_%s' % CURRENCY]), CURRENCY.upper()).format('en_US', '$###,###'),
                colorize(float(coin["percent_change_1h"])),
                colorize(float(coin["percent_change_24h"])),
                colorize(float(coin["percent_change_7d"])),
                humanize(Money(float(coin['market_cap_%s' % CURRENCY]), CURRENCY.upper()))
            ])

    header = ["Rank", "Symbol", "Price", "1h", "24h", "7d", "Mcap"]
    col_widths = [len(n) for n in header]
    for row in rows:
        col_widths = [max(col_widths[n], get_width(row[n])) for n in range(0,len(row))]

    print("    %sWatching (%s)%s" %(bcolors.BOLD, CURRENCY, bcolors.ENDC))
    print("    " +  "".join(justify(
        header[n], col_widths[n]+2) for n in range(0,len(header))))
    for row in sorted(rows, key=lambda x: int(x[0])):
        print("    " + "".join(justify(
            row[n], col_widths[n]+2) for n in range(0,len(row))))

#----------------------------------------------------------------------
def show_portfolio(portfolio):
    total = 0.0
    rows = []
    profit = Money(0.0, CURRENCY.upper())
    # Build table data
    for hold in portfolio:
        for coin in db.tickers.find():
            if coin['symbol'] != hold['symbol']:
                continue

            total += hold['amount'] * float(coin['price_%s' % CURRENCY])

            rows.append([
                coin['rank'],
                coin['symbol'],
                Money(float(coin['price_%s' % CURRENCY]), CURRENCY.upper()),
                humanize(Money(float(coin['market_cap_%s' % CURRENCY]), CURRENCY.upper())),
                hold['amount'],
                Money(round(hold['amount'] * float(coin['price_%s' % CURRENCY]),2),CURRENCY.upper()), # Value
                "", # Portion %
                colorize(float(coin["percent_change_1h"])),
                colorize(float(coin["percent_change_24h"])),
                colorize(float(coin["percent_change_7d"]))
            ])

            profit += Decimal(float(coin['percent_change_24h'])/100) * rows[-1][5]

    rows = sorted(rows, key=lambda x: int(x[5]))[::-1]
    total = Money(total, CURRENCY.upper())
    header = ['Rank', 'Symbol', 'Price', 'Mcap', 'Amount', 'Value', 'Portion', '1h', '24h', '7d']
    col_widths = [len(n) for n in header]

    for row in rows:
        row[6] = str(round((row[5] / total) * 100, 2)) + '%'
        row[2] = row[2].format('en_US', '$###,###')
        row[5] = row[5].format('en_US', '$###,###')

        col_widths = [max(col_widths[n], get_width(row[n])) for n in range(0,len(row))]

    print("\n    %sPortfolio (%s)%s" % (bcolors.BOLD, CURRENCY, bcolors.ENDC))
    print("    " + "".join(justify(
        header[n], col_widths[n]+2) for n in range(0,len(header))))
    for row in rows: #sorted(rows, key=lambda x: int(x[0])):
        print("    " + "".join(justify(
            str(row[n]), col_widths[n]+2) for n in range(0,len(row))))
    print("") #    ---------------------------------------------------------")
    print("    %s$%s%s (%s%s%s)" % (
        bcolors.BOLD, total.format('en_US', '###,###'), bcolors.ENDC,
        bcolors.BOLD, colorize(profit), bcolors.ENDC))

#----------------------------------------------------------------------
def show_spinner(freq):
    i=0
    while i < freq:
        msg = '%s' % next(spinner)
        sys.stdout.write(msg)
        sys.stdout.flush()
        sys.stdout.write('\b'*len(msg))
        i+=1
        sleep(1)

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
