# Display formatted text to stdout in table form
import curses, logging, re
from curses import init_pair, color_pair
from datetime import datetime
from money import Money
from decimal import Decimal
from config import *
from config import CURRENCY as cur
from app import db, analyze
log = logging.getLogger(__name__)

class c:
    BOLD = curses.A_BOLD

#----------------------------------------------------------------------
def markets(stdscr):
    colspace=3
    indent=2
    hdr = ['Market Cap', '24h Vol', 'BTC Cap %', 'Markets', 'Currencies', 'Assets', '1h', '24h', '7d']

    mktdata = list(db.coinmktcap_markets.find().limit(1).sort('datetime',-1))
    if len(mktdata) == 0:
        log.info("db.coinmktcap_markets empty")
        return False

    strrows=[]
    for mkt in mktdata:
        strrows.append([
            pretty(mkt['mktcap_cad'], t="money", abbr=True),
            pretty(mkt['vol_24h_cad'], t="money", abbr=True),
            pretty(mkt['pct_mktcap_btc'], t="pct"),
            pretty(mkt['n_markets']),
            pretty(mkt['n_currencies']),
            pretty(mkt['n_assets']),
            pretty(analyze.mcap_diff('1H', convert='pct'), t="pct", f="sign"),
            pretty(analyze.mcap_diff('24H', convert='pct'), t="pct", f="sign"),
            pretty(analyze.mcap_diff('7D', convert='pct'), t="pct", f="sign")
        ])
    colwidths = _colsizes(hdr, strrows)

    stdscr.clear()
    # Print Title row
    stdscr.addstr(0, indent, "Global (%s)" % cur.upper())
    dt = mktdata[0]["datetime"].strftime("%I:%M %p")
    stdscr.addstr(0, stdscr.getmaxyx()[1] - len(dt) -2, dt)
    # Print Datatable
    printrow(stdscr, 2, hdr, colwidths, [c.WHITE for n in hdr], colspace)
    divider(stdscr, 3, colwidths, colspace)
    for i in range(0, len(strrows)):
        row = strrows[i]
        colors = [c.WHITE for n in range(0,6)] + [pnlcolor(row[n]) for n in range(6,9)]
        printrow(stdscr, i+4, row, colwidths, colors, colspace)
    # Print footer
    #footer(stdscr)

#----------------------------------------------------------------------
def watchlist(stdscr):
    hdr = ["Rank", "Sym", "Price", "Market Cap", "24h Vol", "1h", "24h", "7d"]
    indent=2
    colspace=3
    watchlist = db.user_watchlist.find()
    tickers = list(db.coinmktcap_tickers.find())

    if len(tickers) == 0:
        log.error("coinmktcap collection empty")
        return False

    strrows = []
    for watch in watchlist:
        for tckr in tickers:
            if tckr['id'] != watch['id']:
                continue
            strrows.append([
                tckr["rank"],
                tckr["symbol"],
                pretty(tckr["price_cad"], t='money'),
                pretty(tckr["mktcap_cad"], t='money', abbr=True),
                pretty(tckr["vol_24h_cad"], t='money', abbr=True),
                pretty(tckr["pct_1h"], t='pct', f='sign'),
                pretty(tckr["pct_24h"], t='pct', f='sign'),
                pretty(tckr["pct_7d"], t='pct', f='sign')
            ])
    colwidths = _colsizes(hdr, strrows)
    strrows = sorted(strrows, key=lambda x: int(x[0])) #[::-1]
    stdscr.clear()

    # Print Title row
    stdscr.addstr(0, indent, "Watchlist (%s)" % cur.upper())
    dt = tickers[0]["datetime"].strftime("%I:%M %p")
    stdscr.addstr(0, stdscr.getmaxyx()[1] - len(dt) -2, dt)

    # Print Datatable
    printrow(stdscr, 2, hdr, colwidths, [c.WHITE for n in hdr], colspace)
    divider(stdscr, 3, colwidths, colspace)
    for n in range(0, len(strrows)):
        row = strrows[n]
        colors = [c.WHITE, c.WHITE, c.WHITE, c.WHITE, c.WHITE, pnlcolor(row[5]), pnlcolor(row[6]), pnlcolor(row[7])]
        printrow(stdscr, n+4, row, colwidths, colors, colspace)

    # Print footer
    #footer(stdscr)

#-----------------------------------------------------------------------------
def portfolio(stdscr):
    hdr = ['Rank', 'Sym', 'Price', 'Mcap', 'Amount', 'Value', '%/100', '1h', '24h', '7d']
    indent = 2
    total = 0.0
    portfolio = db.user_portfolio.find()
    datarows = []
    profit = 0

    # Build table data
    tickers = list(db.coinmktcap_tickers.find())
    for hold in portfolio:
        for tckr in tickers:
            if tckr['symbol'] != hold['symbol']:
                continue

            value = round(hold['amount'] * tckr['price_cad'], 2)
            profit += (tckr['pct_24h']/ 100) * value if tckr['pct_24h'] else 0.0
            total += value

            datarows.append([
                tckr['rank'], tckr['symbol'], round(tckr['price_cad'],2), tckr['mktcap_cad'],
                hold['amount'], value, None, tckr["pct_1h"], tckr["pct_24h"], tckr["pct_7d"]
            ])

    # Calculate porfolio %
    for datarow in datarows:
        datarow[6] = round((float(datarow[5]) / total)*100, 2)
    # Sort by holding %
    datarows = sorted(datarows, key=lambda x: int(x[5]))[::-1]

    strrows = []
    for datarow in datarows:
        strrows.append([
            datarow[0],
            datarow[1],
            pretty(datarow[2], t='money'),
            pretty(datarow[3], t='money', abbr=True),
            pretty(datarow[4], abbr=True),
            pretty(datarow[5], t='money'),
            pretty(datarow[6], t='pct'),
            pretty(datarow[7], t='pct', f='sign'),
            pretty(datarow[8], t='pct', f='sign'),
            pretty(datarow[9], t='pct', f='sign')
        ])
    colwidths = _colsizes(hdr, strrows)

    stdscr.clear()

    # Print title Row
    stdscr.addstr(0, indent, "Portfolio (%s)" % cur.upper())
    dt = tickers[0]["datetime"].strftime("%I:%M %p")
    stdscr.addstr(0, stdscr.getmaxyx()[1] - len(dt) -2, dt)

    # Print Datatable
    colspace=3
    printrow(stdscr, 2, hdr, colwidths, [c.WHITE for n in hdr], colspace=3)
    divider(stdscr, 3, colwidths, colspace)
    for y in range(0,len(strrows)):
        strrow = strrows[y]
        colors = [c.WHITE for x in range(0,7)] + [pnlcolor(strrow[n]) for n in range(7,10)]
        printrow(stdscr, y+4, strrow, colwidths, colors, colspace=3)

    # Portfolio value ($)
    printrow(
        stdscr,
        stdscr.getyx()[0]+2,
        [ "Total: ", pretty(total, t='money'), ' (', pretty(int(profit), t="money", f='sign', d=0), ')' ],
        [ 0,0,0,0,0 ],
        [ c.WHITE, c.BOLD, c.WHITE, pnlcolor(profit), c.WHITE ])

    # Print footer
    #footer(stdscr)

#----------------------------------------------------------------------
def printrow(stdscr, y, datarow, colsizes, colors, colspace=2):
    stdscr.move(y,2)
    for idx in range(0, len(datarow)):
        stdscr.addstr(
            y,
            stdscr.getyx()[1],# + colspace, #+1,
            str(datarow[idx]).ljust(colsizes[idx]+colspace),
            colors[idx])

#----------------------------------------------------------------------
def pretty(number, t=None, f=None, abbr=None, d=None):
    """Convert Decimal and floats to human readable strings
    @t: "pct", "money"
    @f: "sign"
    """
    try:
        number = float(number)
    except (ValueError, TypeError) as e:
        return "--"

    head = ""
    tail = ""
    decimal = d if d else 2

    if abbr == True:
        if isinstance(number, Decimal):
            exp = number.adjusted()
        else:
            number = round(number, 2)
            exp = len(str(int(number))) - 1

        if exp in range(0,3):
            strval = str(number)
        elif exp in range(3,6):
            strval = "%s%s" %(round(number/pow(10,3), 2), 'K')
        elif exp in range(6,9):
            strval = "%s%s" %(round(number/pow(10,6), 2), 'M')
        elif exp in range(9,12):
            strval = "%s%s" %(round(number/pow(10,9), 2), 'B')
        elif exp in range(12,15):
            strval = "%s%s" %(round(number/pow(10,12), 2), 'T')
    # Full length number w/ comma separators
    else:
        strval = "{:,}".format(round(number,decimal))

    if f == 'sign':
        head += "+" if number > 0 else ""

    if t == "money":
        head += "$"
    elif t == "pct":
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
    colspace = 3
    widths = [len(n) for n in hdr]
    for row in rows:
        widths = [max(widths[n], len(str(row[n]))) for n in range(0,len(row))]
    return widths

#----------------------------------------------------------------------
def divider(stdscr, y, colwidths, colspace):
    stdscr.hline(y, 2, '-', sum(colwidths) + (len(colwidths)-1)*colspace)

#----------------------------------------------------------------------
def footer(stdscr):
    printrow(stdscr, stdscr.getyx()[0]+3,
        ["Go: '", "M", "'arkets '", "P", "'ortfolio '", "W", "'atchlist '", "D", "'evmode"],
        [0,0,0,0,0,0,0,0,0],
        [c.WHITE, c.BOLD, c.WHITE, c.BOLD, c.WHITE, c.BOLD, c.WHITE, c.BOLD, c.WHITE],
        0)
