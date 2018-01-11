# Display formatted text to stdout in table form
import itertools, re, sys, time
from datetime import datetime
from time import sleep
from money import Money
from decimal import Decimal


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

#----------------------------------------------------------------------
def show_markets(data):
    row = [
        humanize(Money(data['total_market_cap_cad']+0.1, 'CAD')),
        humanize(Money(data['total_24h_volume_cad']+0.1, 'CAD')),
        str(round(data['bitcoin_percentage_of_market_cap'],2))+'%',
        str(data['active_currencies'])
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
def show_watchlist(watchlist, data):
    rows = []
    for watch in watchlist:
        for coin in data:
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
def show_portfolio(portfolio, data):
    total = 0.0
    rows = []
    profit = Money(0.0, 'CAD')
    # Build table data
    for hold in portfolio:
        for coin in data:
            if coin['symbol'] != hold['symbol']:
                continue

            total += hold['amount'] * float(coin['price_cad'])

            rows.append([
                coin['rank'],
                coin['symbol'],
                Money(float(coin['price_cad']), 'CAD'),
                humanize(Money(float(coin['market_cap_cad']), 'CAD')),
                hold['amount'],
                Money(round(hold['amount'] * float(coin['price_cad']),2),'CAD'), # Value
                "", # Portion %
                colorize(float(coin["percent_change_1h"])),
                colorize(float(coin["percent_change_24h"])),
                colorize(float(coin["percent_change_7d"]))
            ])

            profit += Decimal(float(coin['percent_change_24h'])/100) * rows[-1][5]

    rows = sorted(rows, key=lambda x: int(x[5]))[::-1]
    total = Money(total, 'CAD')
    header = ['Rank', 'Symbol', 'Price', 'Mcap', 'Amount', 'Value', 'Portion', '1h', '24h', '7d']
    col_widths = [len(n) for n in header]

    for row in rows:
        row[6] = str(round((row[5] / total) * 100, 2)) + '%'
        row[2] = row[2].format('en_US', '$###,###')
        row[5] = row[5].format('en_US', '$###,###')

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
