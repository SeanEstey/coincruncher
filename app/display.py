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
    mktdata = list(db.coinmktcap_markets.find().sort('_id',-1))
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
    updated = "Updated %s" % mktdata[0]['datetime'].strftime("%I:%M %p")
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
    watchlist = db.user_watchlist.find()
    tickers = list(db.coinmktcap_tickers.find())

    for watch in watchlist:
        for tckr in tickers:
            if tckr['id'] != watch['id']: continue
            rows.append([
                tckr['rank'],
                tckr['symbol'],
                Money(tckr['price_cad'],'CAD').format('en_US', '$###,###'),
                tckr["pct_1h"],
                tckr["pct_24h"],
                tckr["pct_7d"],
                pretty(tckr['mktcap_cad'], t="money", abbr=True),
                pretty(tckr['vol_24h_cad'], t="money", abbr=True)
            ])

    # Temp
    btcfinex = list(db.bitfinex_tickers.find().sort('_id',-1).limit(1))
    if len(btcfinex) > 0:
        btcfinex = btcfinex[0]
        rows.append([
            1, "BTC_BITFNX",
            Money(btcfinex["price_cad"],'CAD').format('en_US','$###,###'), 0.0,
            btcfinex["pct_24h"], 0.0, "",
            pretty(btcfinex["vol_24h_cad"],t="money", abbr=True)
        ])

    colwidths = _colsizes(hdr, rows)

    stdscr.clear()

    # Print Top row
    mktdata = list(db.coinmktcap_markets.find().limit(1).sort('_id',-1))[0]
    updated = "Updated %s" % mktdata['datetime'].strftime("%I:%M %p")
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
    hdr = ['Rank', 'Symbol', 'Price', 'Mcap', 'Amount', 'Value', 'Portion', '1h', '24h', '7d']
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
            profit += (tckr['pct_24h'] / 100) * value
            total += value

            datarows.append([
                tckr['rank'], tckr['symbol'], round(tckr['price_cad'],2), round(tckr['mktcap_cad'],2),
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
    stdscr.addstr(1, indent, "Portfolio (%s)" % cur.upper())
    dt = tickers[0]["datetime"].strftime("%I:%M %p")
    stdscr.addstr(1, stdscr.getmaxyx()[1] - len(dt) -2, dt)

    # Print datatable
    printrow(stdscr, 3, hdr, colwidths, [c.WHITE for n in hdr])
    stdscr.hline(4, 2, '-', curses.COLS-4)
    for y in range(0,len(strrows)):
        strrow = strrows[y]
        colors = [c.BOLD for x in range(0,7)] + [pnlcolor(strrow[n]) for n in range(7,10)]
        printrow(stdscr, y+5, strrow, colwidths, colors)

    # Portfolio value ($)
    printrow(
        stdscr,
        stdscr.getyx()[0]+2,
        [ "Total: ", pretty(total, t='money'), ' (', pretty(int(profit), t="money", f='sign', d=0), ')' ],
        [ 0,0,0,0,0 ],
        [ c.WHITE, c.BOLD, c.WHITE, pnlcolor(profit), c.WHITE ])

    # Print footer
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
    stdscr.move(y,2)
    for idx in range(0, len(datarow)):
        stdscr.addstr(
            y,
            stdscr.getyx()[1],#+1,
            str(datarow[idx]).ljust(colsizes[idx]),
            colors[idx])

#----------------------------------------------------------------------
def pretty(number, t=None, f=None, abbr=None, d=None):
    """Convert Decimal and floats to human readable strings
    @t: "pct", "money"
    @f: "sign"
    """
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
    widths = [len(str(n)) for n in hdr]
    for row in rows:
        widths = [max(widths[n], len(str(row[n]))+colspace) for n in range(0,len(row))]
    return widths
