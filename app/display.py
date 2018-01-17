# Display formatted text to stdout in table form
import curses, logging, re
from curses import init_pair, color_pair
from datetime import datetime
from money import Money
from decimal import Decimal
from config import *
from config import CURRENCY as cur
from app import db
log = logging.getLogger(__name__)

class c:
    BOLD = curses.A_BOLD

#----------------------------------------------------------------------
def markets(stdscr):
    return True

    stdscr.clear()
    indent=2
    mktdata = list(db.markets.find().sort('_id',-1))
    if len(mktdata) == 0:
        log.info("db.markets empty. Waiting on thread...")
        return False
    else:
        pass
        # Sort descending
        #mktdata = mktdata[::-1]

    # Print header
    hdr = ['Datetime', 'MCap', '24h Volume', 'BTC Score', 'Markets', 'Currencies', 'Assets']
    y=1
    stdscr.addstr(1, indent, "Markets (%s)" % cur.upper())
    height,width = stdscr.getmaxyx()
    updated = "Updated %s" % datetime.fromtimestamp(mktdata[0]['timestamp']).strftime("%I:%M %p")
    stdscr.addstr(1, width - len(updated) -2, updated)
    #stdscr.addstr(3, indent, "".join(justify(hdr[n], widths[n]+2) for n in range(0,len(hdr))))

    max_rows = max(stdscr.getmaxyx()[0]-4, len(mktdata))
    datarows=[]
    for entry in mktdata:
        datarows.append([
            pretty(mktdata['mktcap_cad'], t="money"),
            pretty(mktdata['vol_24h_cad'], t="money"),
            pretty(mktdata['pct_mktcap_btc'], t="pct"),
            str(mktdata['n_markets']),
            str(mktdata['n_currencies']),
            str(mktdata['n_assets'])
        ])

    widths = [len(n) for n in hdr]
    widths = [max(widths[n], get_width(row[n])) for n in range(0,len(row))]

    #stdscr.addstr(4, indent, "".join(justify(row[n], widths[n]+2) for n in range(0,len(row))), c.BOLD)
    footer(stdscr)

#----------------------------------------------------------------------
def watchlist(stdscr):
    hdr = ["Rank", "Symbol", "Price", "1h", "24h", "7d", "Mcap", "24h Vol"]
    rows = []
    indent=2

    stdscr.clear()
    watchlist = db.watchlist.find()

    for watch in watchlist:
        for coin in db.tickers.find():
            if coin['id'] != watch['id']: continue
            rows.append([
                coin['rank'],
                coin['symbol'],
                Money(coin['price_cad'],'CAD').format('en_US', '$###,###'),
                coin["pct_1h"],
                coin["pct_24h"],
                coin["pct_7d"],
                pretty(coin['mktcap_cad'], t="money"),
                pretty(coin['vol_24h_cad'], t="money")
            ])
    colwidths = _colsizes(hdr, rows)

    # Print Top row
    mktdata = list(db.markets.find().limit(1).sort('_id',-1))[0]
    updated = "Updated %s" % datetime.fromtimestamp(mktdata['timestamp']).strftime("%I:%M %p")
    stdscr.addstr(1, indent, "Watchlist (%s)" % cur.upper())
    stdscr.addstr(1, stdscr.getmaxyx()[1] - len(updated) -2, updated)

    # Print Header row
    printrow(stdscr, 3, hdr, colwidths, [c.WHITE for n in hdr])

    # Print Data rows
    for n in range(0, len(rows)):
        row = rows[n]
        colors = [c.BOLD, c.BOLD, c.BOLD, pnlcolor(row[3]), pnlcolor(row[4]), pnlcolor(row[5]), c.BOLD, c.BOLD]
        printrow(stdscr, n+4, row, colwidths, colors)

    # Print footer
    footer(stdscr)

#-----------------------------------------------------------------------------
def portfolio(stdscr):
    return True

    stdscr.clear()
    indent = 2
    total = 0.0
    portfolio = db.portfolio.find()
    rows = []
    profit = Money(0.0, cur.upper())

    # Build table data
    for hold in portfolio:
        for coin in db.tickers.find():
            if coin['symbol'] != hold['symbol']: continue
            value = round(hold['amount'] * coin['price_cad'], 2)
            total += value
            profit += Decimal((coin['pct_24h']/100) * value)
            rows.append([
                coin['rank'],
                coin['symbol'],
                pretty(coin['price_cad'], t="money"),
                pretty(coin['mktcap_cad'], t="money"),
                hold['amount'],
                pretty(Money(value,'CAD'), t="money"),
                "",
                pretty(coin["pct_1h"], t="pct", f="sign"),
                pretty(coin["pct_24h"], t="pct", f="sign"),
                pretty(coin["pct_7d"], t="pct", f="sign")
            ])

    rows = sorted(rows, key=lambda x: int(x[5]))[::-1]
    total = Money(total, cur.upper())
    hdr = ['Rank', 'Symbol', 'Price', 'Mcap', 'Amount', 'Value', 'Portion', '1h', '24h', '7d']
    widths = [len(n) for n in hdr]

    """for row in rows:

        printrow(stdscr, line, data, colors):
    """

    for row in rows:
        row[6] = str(round((row[5] / total) * 100, 2)) + '%'
        row[2] = row[2].format('en_US', '$###,###')
        row[5] = row[5].format('en_US', '$###,###')
        widths = [max(widths[n], get_width(row[n])) for n in range(0,len(row))]

    stdscr.addstr(1, indent, "Portfolio (%s)" % cur.upper())
    scr_height,scr_width = stdscr.getmaxyx()
    mktdata = list(db.markets.find().limit(1).sort('_id',-1))[0]
    updated = "Updated %s" % datetime.fromtimestamp(mktdata['last_updated']).strftime("%I:%M %p")
    stdscr.addstr(1, scr_width - len(updated) -2, updated)
    #stdscr.addstr(3, indent, "".join(justify(hdr[n], widths[n]+2) for n in range(0,len(hdr))))
    y = 4
    for row in rows:
        x = 2
        for col_idx in range(0, len(row)): #width in widths:
            val = row[col_idx]
            if col_idx in [7,8,9]:
                stdscr.addstr(y, x, "%s%s" %("+" if val > 0 else "", str(val)+"%"), c.GREEN if val>0 else c.RED)
                x += 1
            else:
                stdscr.addstr(y, x, str(val), c.BOLD)
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
    footer(stdscr)

#----------------------------------------------------------------------
def footer(stdscr):
    indentx=2
    y = stdscr.getmaxyx()[0] - 1
    stdscr.addstr(y, indentx, "commands: ")
    stdscr.addstr(y, stdscr.getyx()[1], "m", c.BOLD)
    stdscr.addstr(y, stdscr.getyx()[1], "arkets, ")
    stdscr.addstr(y, stdscr.getyx()[1], "p", c.BOLD)
    stdscr.addstr(y, stdscr.getyx()[1], "ortfolio, ")
    stdscr.addstr(y, stdscr.getyx()[1], "w", c.BOLD)
    stdscr.addstr(y, stdscr.getyx()[1], "atchlist, ")
    stdscr.addstr(y, stdscr.getyx()[1], "d", c.BOLD)
    stdscr.addstr(y, stdscr.getyx()[1], "eveloper mode")

#----------------------------------------------------------------------
def printrow(stdscr, y, datarow, colsizes, colors):
    #log.info("colsizes: %s", colsizes)
    #log.info("colors: %s", colors)
    #log.info("data: %s", datarow)
    stdscr.move(y,2)

    for idx in range(0, len(datarow)):
        stdscr.addstr(
            y,
            stdscr.getyx()[1]+1,
            str(datarow[idx]).ljust(colsizes[idx]+2),
            colors[idx])

#----------------------------------------------------------------------
def pretty(number, t=None, f=None):
    """Convert Decimal and floats to human readable strings
    @t: "pct", "money"
    @f: "signed"
    """
    head = ""
    tail = ""

    if isinstance(number, Decimal):
        number = float(number)
        exp = number.adjusted()
    elif type(number) == float or type(number) == int:
        number = round(number, 2)
        exp = len(str(int(number))) - 1

    if f == 'signed':
        head += "+" if number > 0 else ""

    if t == "money":
        head += "$"

    if exp in range(0,3):
        strval = str(number)
    elif exp in range(3,6):
        strval = "%s%s" %(round(number/pow(10,3),2), 'thousand')
        tail += " thousand"
    elif exp in range(6,9):
        strval = "%s%s" %(round(number/pow(10,6),2), 'M')
    elif exp in range(9,12):
        strval = "%s%s" %(round(number/pow(10,9),2), 'B')
    elif exp in range(12,15):
        strval = "%s%s" %(round(number/pow(10,12),2), 'T')

    if t == "pct":
        tail += "%"

    return "%s%s%s" %(head, strval, tail)

#----------------------------------------------------------------------
def pnlcolor(colorstr):
    return c.RED if str(colorstr)[0] == '-' else c.GREEN

#----------------------------------------------------------------------
def get_width(val):
    rmv_esc = re.compile(r'\x1b[^m]*m')
    fixed_len = rmv_esc.sub('', str(val))
    return len(fixed_len)

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

#-----------------------------------------------------------------------------
def _colsizes(hdr, rows):
    widths = [len(n) for n in hdr]
    for row in rows:
        widths = [max(widths[n], len(str(row[n]))) for n in range(0,len(row))]
    return widths
