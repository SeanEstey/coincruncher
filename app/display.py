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
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class c:
    BOLD = curses.A_BOLD

#----------------------------------------------------------------------
def _print_color_palette(stdscr):
    try:
        for i in range(0, 255):
            stdscr.addstr(str(i), color_pair(i))
    except curses.ERR:
        pass

#----------------------------------------------------------------------
def set_colors(stdscr):
    """ init_pair args: [pair_number, foreground, background]
    pair_number 0 = WHITE
    """
    curses.start_color()
    curses.use_default_colors()

    for i in range(0, curses.COLORS):
        init_pair(i + 1, i, -1)

    c.WHITE = color_pair(0)
    c.ORANGE = color_pair(2)
    c.GREEN = color_pair(4)
    c.BLUE = color_pair(5)
    c.RED = color_pair(10)

#----------------------------------------------------------------------
def show_markets(stdscr):
    stdscr.clear()
    indent=2
    markets = list(db.markets.find().limit(1).sort('_id',-1))[0]

    row = [
        humanize(Money(markets['total_market_cap_%s' % CURRENCY], CURRENCY.upper())),
        humanize(Money(markets['total_24h_volume_%s' % CURRENCY], CURRENCY.upper())),
        str(round(markets['bitcoin_percentage_of_market_cap'],2))+'%',
        str(markets['active_currencies'])
    ]
    hdr = ['Market Cap', '24h Volume', 'BTC Dominance', 'Currencies']
    widths = [len(n) for n in hdr]
    widths = [max(widths[n], get_width(row[n])) for n in range(0,len(row))]

    #stdscr.addstr('Refreshed %s' % datetime.now().strftime("%H:%M:%S"))
    #stdscr.addstr(y, x, "Global (%s)%s" % (
    #    datetime.now().strftime("%h %d %H:%M:%S"), c.WHITE, CURRENCY, c.WHITE))

    y=1
    stdscr.addstr(1, indent, "Global (%s)" % CURRENCY.upper(), c.BOLD)
    stdscr.addstr(3, indent, "".join(justify(hdr[n], widths[n]+2) for n in range(0,len(hdr))))
    stdscr.addstr(4, indent, "".join(justify(row[n], widths[n]+2) for n in range(0,len(row))))

#----------------------------------------------------------------------
def show_watchlist(stdscr):
    stdscr.clear()
    indent=2
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
                float(coin["percent_change_1h"]),
                float(coin["percent_change_24h"]),
                float(coin["percent_change_7d"]),
                humanize(Money(float(coin['market_cap_%s' % CURRENCY]), CURRENCY.upper())),
                humanize(Money(float(coin['24h_volume_%s' % CURRENCY]), CURRENCY.upper()))
            ])

    hdr = ["Rank", "Symbol", "Price", "1h", "24h", "7d", "Mcap", "24h Vol"]
    widths = [len(n) for n in hdr]
    for row in rows:
        widths = [max(widths[n], get_width(row[n])) for n in range(0,len(row))]

    stdscr.addstr(1, indent, "Watchlist (%s)" % CURRENCY.upper(), c.BOLD)
    stdscr.addstr(3, indent, "".join(justify(hdr[n], widths[n]+2) for n in range(0,len(hdr))))

    y=4
    for row in sorted(rows, key=lambda x: int(x[0])):
        x = 2
        for col_idx in range(0, len(row)):
            val = row[col_idx]
            if col_idx in [3,4,5]:
                stdscr.addstr(y, x, "%s%s" %("+" if val > 0 else "", str(val)+"%"), c.GREEN if val>0 else c.RED)
                x += 1
            else:
                stdscr.addstr(y, x, str(val))
            x += widths[col_idx] +2
        y += 1

#----------------------------------------------------------------------
def show_portfolio(stdscr):
    stdscr.clear()
    indent = 2
    total = 0.0
    portfolio = db.portfolio.find()
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
                float(coin["percent_change_1h"]),
                float(coin["percent_change_24h"]),
                float(coin["percent_change_7d"])
            ])
            profit += Decimal(float(coin['percent_change_24h'])/100) * rows[-1][5]

    rows = sorted(rows, key=lambda x: int(x[5]))[::-1]
    total = Money(total, CURRENCY.upper())
    hdr = ['Rank', 'Symbol', 'Price', 'Mcap', 'Amount', 'Value', 'Portion', '1h', '24h', '7d']
    widths = [len(n) for n in hdr]

    for row in rows:
        row[6] = str(round((row[5] / total) * 100, 2)) + '%'
        row[2] = row[2].format('en_US', '$###,###')
        row[5] = row[5].format('en_US', '$###,###')
        widths = [max(widths[n], get_width(row[n])) for n in range(0,len(row))]

    stdscr.addstr(1, indent, "Portfolio (%s)" % CURRENCY.upper(), c.BOLD)
    stdscr.addstr(3, indent, "".join(justify(hdr[n], widths[n]+2) for n in range(0,len(hdr))))
    y = 4
    for row in rows:
        x = 2
        for col_idx in range(0, len(row)): #width in widths:
            val = row[col_idx]
            if col_idx in [7,8,9]:
                stdscr.addstr(y, x, "%s%s" %("+" if val > 0 else "", str(val)+"%"), c.GREEN if val>0 else c.RED)
                x += 1
            else:
                stdscr.addstr(y, x, str(val))
            x += widths[col_idx] +2
        y += 1
    # Total portfolio value
    stdscr.addstr(y+1, indent, "$%s" % total.format('en_US', '###,###'), c.BOLD)
    # 24h profit/loss
    curs = stdscr.getyx()
    stdscr.addstr(curs[0], curs[1]+1, "(")
    curs = stdscr.getyx()
    stdscr.addstr(curs[0], curs[1], "%s%s" %(
        "+" if profit.amount > 0 else "",
        profit.format('en_US', '$###,###')),
        c.GREEN if profit.amount > 0 else c.RED)
    curs = stdscr.getyx()
    stdscr.addstr(curs[0], curs[1], ")")

#----------------------------------------------------------------------
def printscr(msg, *args):
    # Split up args tuple by color args (ints)
    # Call stdscr.addstr() for each color
    pass

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
            c.RED if val.amount < 0 else c.GREEN,
            "+" if val.amount > 0 else "",
            val.format('en_US', '###,###'),
            c.WHITE)
    elif type(val) == float:
        return "%s%s%s%s" %(
            c.RED if val < 0 else c.GREEN,
            "+" if val > 0 else "",
            str(round(val,1)) + '%',
            c.WHITE)

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
