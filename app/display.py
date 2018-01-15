# Display formatted text to stdout in table form
import curses, itertools, logging, re, sys, time
from curses import init_pair, color_pair
from datetime import datetime
from time import sleep
from money import Money
from decimal import Decimal
from config import *
from app import db
log = logging.getLogger(__name__)

spinner = itertools.cycle(['-', '/', '|', '\\'])
"""class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
"""



class bcolors:
    WHITE = curses.COLOR_WHITE
    GREEN = curses.COLOR_GREEN
    RED = curses.COLOR_RED
    BOLD = curses.COLOR_WHITE

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

    print(chr(27) + "[2J")
    print('Refreshed %s' % datetime.now().strftime("%H:%M:%S"))

    print("\n    %s\n\n    %sGlobal (%s)%s" % (
        datetime.now().strftime("%h %d %H:%M:%S"), bcolors.WHITE, CURRENCY, bcolors.WHITE))
    print("    " + "".join(justify(header[n], col_widths[n]+2) for n in range(0,len(header))))
    print("    " + "".join(justify(row[n], col_widths[n]+2) for n in range(0,len(row))))
    print("")

#----------------------------------------------------------------------
def show_watchlist():
    watchlist = db.watchlist.find()
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
                humanize(Money(float(coin['market_cap_%s' % CURRENCY]), CURRENCY.upper())),
                humanize(Money(float(coin['24h_volume_%s' % CURRENCY]), CURRENCY.upper()))
            ])

    header = ["Rank", "Symbol", "Price", "1h", "24h", "7d", "Mcap", "24h Vol"]
    col_widths = [len(n) for n in header]
    for row in rows:
        col_widths = [max(col_widths[n], get_width(row[n])) for n in range(0,len(row))]

    print(chr(27) + "[2J")
    print('Refreshed %s' % datetime.now().strftime("%H:%M:%S"))

    print("    %sWatching (%s)%s\n" %(bcolors.WHITE, CURRENCY.upper(), bcolors.WHITE))
    print("    " +  "".join(justify(
        header[n], col_widths[n]+2) for n in range(0,len(header))))
    for row in sorted(rows, key=lambda x: int(x[0])):
        print("    " + "".join(justify(
            row[n], col_widths[n]+2) for n in range(0,len(row))))

#----------------------------------------------------------------------
def show_portfolio(stdscr):
    stdscr.clear()

    portfolio = db.portfolio.find()
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

    stdscr.addstr(1,1, 'Refreshed %s' % datetime.now().strftime("%H:%M:%S"))
    stdscr.addstr(3,1, "\n    %sPortfolio (%s)%s\n" % (bcolors.WHITE, CURRENCY.upper(), bcolors.WHITE))
    stdscr.addstr(5,1, "    " + "".join(justify(
        header[n], col_widths[n]+2) for n in range(0,len(header))))
    line = 6
    for row in rows:
        stdscr.addstr(line, 1, "    " + "".join(justify(
            str(row[n]), col_widths[n]+2) for n in range(0,len(row))))
        line += 1
    stdscr.addstr(line, 1, "    %s$%s%s (%s%s%s)" % (
        bcolors.WHITE, total.format('en_US', '###,###'), bcolors.WHITE,
        bcolors.WHITE, colorize(profit), bcolors.WHITE))

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
            bcolors.RED if val.amount < 0 else bcolors.GREEN,
            "+" if val.amount > 0 else "",
            val.format('en_US', '###,###'),
            bcolors.WHITE)
    elif type(val) == float:
        return "%s%s%s%s" %(
            bcolors.RED if val < 0 else bcolors.GREEN,
            "+" if val > 0 else "",
            str(round(val,1)) + '%',
            bcolors.WHITE)

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
