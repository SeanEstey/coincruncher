# Display formatted text to stdout in table form
import curses, logging, re
from curses import init_pair, color_pair
from decimal import Decimal
from config import *
from config import CURRENCY as cur
from app import db
log = logging.getLogger(__name__)

class c:
    BOLD = curses.A_BOLD

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
        ["M", "arkets ", "P", "ortfolio ", "W", "atchlist ", "D", "evmode"],
        [0,0,0,0,0,0,0,0],
        [c.BOLD, c.WHITE, c.BOLD, c.WHITE, c.BOLD, c.WHITE, c.BOLD, c.WHITE],
        0)
