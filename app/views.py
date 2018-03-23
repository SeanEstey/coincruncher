# app.views
import logging
import pandas as pd
import numpy as np
from pprint import pformat, pprint
from decimal import Decimal
from datetime import timedelta, datetime
from pprint import pformat
from app import get_db, forex, markets, signals, tickers
from app.analyze import price_df
from app.utils import utc_dtdate, to_relative_str, to_int, to_dt, utc_datetime
from app.screen import c, midx, print_table, pretty, pnlcolor, coeff_color
from app.timer import Timer
from docs.config import CURRENCY
from docs.data import COINMARKETCAP as CMC
log = logging.getLogger('views')

#-----------------------------------------------------------------------------
def show_home(stdscr):
    db = get_db()
    n_indexed = db.tickers_1d.count() + db.tickers_5t.count() +\
        db.market_idx_1d.count() + db.market_idx_5t.count()
    stdscr.clear()
    stdscr.addstr(0, 2, "%s datapoints indexed" % pretty(n_indexed, abbr=True))

    updated = "Updated 1 min ago" # + to_relative_str(utc_datetime() - mktdata[0]["date"])
    stdscr.addstr(0, stdscr.getmaxyx()[1]-len(updated) - 2, updated)
    stdscr.addstr(3, 0, "")

    title=\
        [" ██████╗ ██████╗ ██╗███╗   ██╗ ██████╗██████╗ ██╗   ██╗███╗   ██╗ ██████╗██╗  ██╗███████╗██████╗ "]+\
        ["██╔════╝██╔═══██╗██║████╗  ██║██╔════╝██╔══██╗██║   ██║████╗  ██║██╔════╝██║  ██║██╔════╝██╔══██╗"]+\
        ["██║     ██║   ██║██║██╔██╗ ██║██║     ██████╔╝██║   ██║██╔██╗ ██║██║     ███████║█████╗  ██████╔╝"]+\
        ["██║     ██║   ██║██║██║╚██╗██║██║     ██╔══██╗██║   ██║██║╚██╗██║██║     ██╔══██║██╔══╝  ██╔══██╗"]+\
        ["╚██████╗╚██████╔╝██║██║ ╚████║╚██████╗██║  ██║╚██████╔╝██║ ╚████║╚██████╗██║  ██║███████╗██║  ██║"]+\
        [" ╚═════╝ ╚═════╝ ╚═╝╚═╝  ╚═══╝ ╚═════╝╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝"]

    for line in title:
        stdscr.addstr(stdscr.getyx()[0]+1, int(stdscr.getmaxyx()[1]/2-len(line)/2), line)

    # Print menu options
    width = stdscr.getmaxyx()[1]
    x = int(width/2) - 10
    stdscr.addstr(stdscr.getyx()[0]+3, x,  "G    Global Market")
    stdscr.addstr(stdscr.getyx()[0]+1, x,  "S    Signals")
    stdscr.addstr(stdscr.getyx()[0]+1, x,  "H    Ticker History")
    stdscr.addstr(stdscr.getyx()[0]+1, x,  "D    Data Patterns")
    stdscr.addstr(stdscr.getyx()[0]+1, x,  "W    My Watchlist")
    stdscr.addstr(stdscr.getyx()[0]+1, x,  "P    My Portfolio")
    stdscr.addstr(stdscr.getyx()[0]+1, x,  "Q    Quit")

#-----------------------------------------------------------------------------
def show_signals(stdscr):
    from app.signals import FREQ_TO_STR, PER_TO_STR

    if stdscr:
        stdscr.clear()

    dfp = signals.load_pairs()

    # Convert int freq/period indices to str representation
    dfa = signals.load_aggregate()[["signal", "age"]]
    #lvl0 = dfa.index.get_level_values(0)
    #lvl1 = dfa.index.get_level_values(1)
    #lvl2 = dfa.index.get_level_values(2)
    #lvl1 = [ FREQ_TO_STR[n] for n in lvl1 ]
    #lvl2 = [ PER_TO_STR[n] for n in lvl2 ]
    #dfa.index = pd.MultiIndex.from_arrays([lvl0, lvl1, lvl2])
    #dfa.index.names = ["Pair","Freq","Period"]

    for idx, row in dfa.iterrows():
        if isinstance(row.age, datetime):
            diff = utc_datetime() - row.age
            hrs = round(diff.total_seconds()/3600, 1)
            if hrs < 1:
                dfa.set_value(idx,"age", str(int(hrs*60))+"m")
            else:
                dfa.set_value(idx,"age", str(hrs)+"h")

    dfa.age = dfa.age.replace(0,"-")
    dfa["signal"] = dfa["signal"].apply(lambda x: "{0:+.1f}".format(x))
    #dfa.columns=["T+/-", "Signal"]
    pairs = list(dfa.index.levels[0])
    pair = pairs[0]

    n=0
    xpos=2
    for i in range(0,5):
        ypos=2
        for j in range(0,4):
            stdscr.addstr(ypos, xpos, pair, c.BOLD) if stdscr else pprint(pair)
            ypos+=1

            lines = pformat(dfa.loc[(pair)], width=100).split("\n")
            if stdscr:
                for line in lines:
                    stdscr.addstr(ypos, xpos, line)
                    ypos+=1
            else:
                pprint(lines)
            ypos+=2
            pair=pairs[pairs.index(pair) + 1]
        xpos+=30

    return dfa

#-----------------------------------------------------------------------------
def show_patterns(stdscr):
    """Ticker correlation matrix data table.
    """
    rng = pd.date_range(
        utc_dtdate()-timedelta(days=1),
        periods=24,
        freq='1H')

    df = price_df(CMC["CORR"], rng)
    corr = df.pct_change().corr().round(2)

    headers = [" "] + corr.index.tolist()
    rows, colors = [], []

    for idx in corr.index.tolist():
        row = [str(round(Decimal(n),2)) for n in corr[idx].tolist()]
        rows.append([idx] + row)
        colors.append([c.WHITE] + [coeff_color(n) for n in corr[idx].tolist()])

    title = "Price Correlation Matrix (CORR30)"
    footer = "Dataset frequency: %s, Datapoints: %s" %('?',len(df))

    stdscr.clear()
    print_table(stdscr, [title], headers, rows, colors,
        align=str.rjust, colsp=2)
    stdscr.addstr(stdscr.getyx()[0]+1, int(midx(stdscr)/2 - len(footer)/2), footer)

    # Biggest winners/losers datatable
    ex = forex.getrate('CAD',utc_dtdate())
    stdscr.addstr(stdscr.getyx()[0]+1, stdscr.getyx()[1], "")
    deviants = tickers.volatile_24h()
    print_table(stdscr,
        ["Winners & Losers in Top 500"],
        ["Coin","Rank","Price","Market Cap","24 Hour"],
        [   [n["symbol"],
            n["rank"],
            pretty(ex * to_int(n["price_usd"]), t='money', abbr=True),
            pretty(ex * to_int(n["mktcap_usd"]), t='money', abbr=True),
            format(n["pct_24h"],"+.2f")+"%"
            ] for n in deviants
        ],
        [   [c.WHITE, c.WHITE, c.WHITE, c.WHITE, pnlcolor(n["pct_24h"])
            ] for n in deviants
        ],
        align=str.rjust, colsp=2)

#-----------------------------------------------------------------------------
def show_markets(stdscr):
    """Global market data.
    """
    _diff = markets.diff
    _to = pretty
    db = get_db()
    stdscr.clear()

    # Latest market (table)
    ex = forex.getrate('CAD',utc_dtdate())
    hdr = ['Market Cap', '24h Volume', 'BTC Dominance', 'Markets', 'Currencies',
           '1 Hour', '24 Hour', '7 Day', '30 Day']
    mktdata = list(db.market_idx_5t.find().limit(1).sort('date',-1))
    if len(mktdata) == 0:
        return log.error("db.market_idx_5t empty")
    rows, colors = [], []
    for mkt in mktdata:
        rows.append([
            _to(ex * mkt['mktcap_usd'], t="money", abbr=True),
            _to(ex * mkt['vol_24h_usd'], t="money", abbr=True),
            _to(mkt['pct_mktcap_btc'], t="pct"),
            _to(mkt['n_markets'], d=0),
            _to(mkt['n_assets'] + mkt['n_currencies'], d=0),
            _to(_diff('1H', to_format='percentage'), t="pct", f="sign"),
            _to(_diff('24H', to_format='percentage'), t="pct", f="sign"),
            _to(_diff('7D', to_format='percentage'), t="pct", f="sign"),
            "-"
        ])
        colors.append([c.WHITE]*5 + [pnlcolor(rows[-1][col]) for col in range(5,9)])

    stdscr.addstr(0, 2, "< Home")
    page_title = "Markets (%s)" % CURRENCY.upper()
    stdscr.addstr(0, int(stdscr.getmaxyx()[1]/2 - len(page_title)/2), page_title)
    updated = "Updated " + to_relative_str(utc_datetime() - mktdata[0]["date"]) + " ago"
    stdscr.addstr(0, stdscr.getmaxyx()[1]-len(updated) - 2, updated)
    stdscr.addstr(2, 0, "")

    print_table(
        stdscr,
        ["Current"],
        hdr, rows, colors, div=True)

    # Weekly market (table)
    start = utc_dtdate() + timedelta(days=-14)
    cursor = db.market_idx_1d.find(
        {"date":{"$gte":start, "$lt":utc_dtdate()}}).sort('date',-1)
    if cursor.count() < 1:
        return log.error("no data for weekly markets")
    hdr = ["Date","Mcap Open", "Mcap High","Mcap Low", "Mcap Close",
          "Mcap Spread", "Mcap SD", "Volume","BTC Dom"]
    rows, colors = [], []

    for mkt in cursor:
        ex = forex.getrate('CAD', mkt["date"])
        rows.append([
            mkt["date"].strftime("%b-%d"),
            _to(ex * mkt["mktcap_open_usd"], t='money', d=1, abbr=True),
            _to(ex * mkt["mktcap_high_usd"], t='money', d=1, abbr=True),
            _to(ex * mkt["mktcap_low_usd"], t='money', d=1, abbr=True),
            _to(ex * mkt["mktcap_close_usd"], t='money', d=1, abbr=True),
            _to(mkt["mktcap_spread_usd"]/mkt["mktcap_low_usd"] * 100, t='pct'),
            _to(ex * mkt["mktcap_std_24h_usd"], t='money', d=1, abbr=True),
            _to(ex * mkt['vol_24h_close_usd'], t="money", d=1, abbr=True),
            _to(ex * mkt['btc_mcap'], t="pct")
        ])
        colors.append([c.WHITE]*9)

    cursor.rewind()
    df = pd.DataFrame(list(cursor))

    colors[df["mktcap_high_usd"].idxmax()][2] = c.GREEN
    colors[df["mktcap_low_usd"].idxmin()][3] = c.RED
    colors[df["vol_24h_close_usd"].idxmax()][7] = c.GREEN

    stdscr.addstr(stdscr.getyx()[0]+1, 0, "")
    print_table(stdscr, ["Recent"], hdr, rows, colors, div=True)

#-----------------------------------------------------------------------------
def show_history(stdscr, symbol):
    log.info("Querying %s ticker history", symbol)
    t1 = Timer()
    db = get_db()

    ex = forex.getrate('CAD',utc_dtdate())
    n_display = 95
    colspace=3
    indent=2
    hdr = ['Date', 'Open', 'High', 'Low', 'Close', 'Market Cap', 'Vol 24h']

    tickerdata = db.tickers_1d.find({"symbol":symbol}
        ).sort('date',-1).limit(n_display)
    n_datarows = tickerdata.count()
    log.debug("%s tickers queried in %sms", tickerdata.count(), t1)

    if tickerdata.count() == 0:
        log.info("No ticker history found for %s", symbol)
        return False
    strrows=[]
    for tck in tickerdata:
        strrows.append([
            tck['date'].strftime("%m-%d-%Y"),
            pretty(tck['open'], t="money"),
            pretty(tck['high'], t="money"),
            pretty(tck['low'], t="money"),
            pretty(tck['close'], t="money"),
            pretty(tck['mktcap_usd'], t="money", abbr=True),
            pretty(tck['vol_24h_usd'], t="money", abbr=True)
        ])
    import curses
    from app.screen import divider, printrow, _colsizes
    colwidths = _colsizes(hdr, strrows)

    stdscr.clear()
    stdscr.addstr(0, indent, "%s History" % symbol)
    printrow(stdscr, 2, hdr, colwidths, [c.WHITE for n in hdr], colspace)

    divider(stdscr, 3, colwidths, colspace)
    for i in range(0, len(strrows)):
        colors = [c.WHITE for n in range(0,7)]
        printrow(stdscr, i+4, strrows[i], colwidths, colors, colspace)

    n_rem_scroll = n_datarows - (curses.LINES - 4)
    log.info("n_datarows=%s, n_rem_scroll=%s", n_datarows, n_rem_scroll)
    return n_rem_scroll

#-----------------------------------------------------------------------------
def show_watchlist(stdscr):
    diff = tickers.diff
    db = get_db()
    ex = forex.getrate('CAD',utc_dtdate())
    rows, colors = [], []
    hdr = ["Rank", "Sym", "Price", "Market Cap", "24h Vol", "1 Hour", "24 Hour",
        "7 Days", "30 Days", "1 Year"]
    updated = []

    for watch in db.watchlist.find():
        cursor = db.tickers_5t.find({"symbol":watch["symbol"]}).sort("date",-1).limit(1)
        if cursor.count() < 1:
            continue
        tckr = cursor.next()
        rows.append([
            tckr["rank"],
            tckr["symbol"],
            tckr["price_usd"],
            tckr["mktcap_usd"],
            tckr["vol_24h_usd"],
            tckr["pct_1h"],
            tckr["pct_24h"],
            tckr["pct_7d"],
            diff(tckr["symbol"], tckr["price_usd"], "30D", to_format="percentage"),
            diff(tckr["symbol"], tckr["price_usd"], "1Y", to_format="percentage")
        ])
        updated.append(tckr["date"].timestamp())

    rows = sorted(rows, key=lambda x: int(x[0]))

    for row in rows:
        colors.append(
            [c.WHITE]*5 +\
            [pnlcolor(row[5]), pnlcolor(row[6]), pnlcolor(row[7]),
            pnlcolor(row[8]), pnlcolor(row[9])]
        )
        row[2] = pretty(ex * row[2], t='money')
        row[3] = pretty(ex * to_int(row[3]), t='money', abbr=True)
        row[4] = pretty(ex * to_int(row[4]), t='money', abbr=True)
        row[5] = pretty(row[5], t='pct', f='sign')
        row[6] = pretty(row[6], t='pct', f='sign')
        row[7] = pretty(row[7], t='pct', f='sign')
        row[8] = pretty(row[8], t='pct', f='sign')
        row[9] = pretty(row[9], t='pct', f='sign')

    # Print
    stdscr.clear()
    updated = to_relative_str(utc_datetime() - to_dt(max(updated))) + " ago"
    stdscr.addstr(0, 2, "Updated %s" % updated)
    stdscr.addstr(0, stdscr.getmaxyx()[1]-5, CURRENCY.upper())
    stdscr.addstr(1, 0, "")
    print_table(stdscr, ["Watchlist"], hdr, rows, colors, div=True)

#-----------------------------------------------------------------------------
def show_portfolio(stdscr):
    diff = tickers.diff
    t1 = Timer()
    db = get_db()
    total = 0.0
    profit = 0
    datarows, updated = [], []
    ex = forex.getrate('CAD',utc_dtdate())
    hdr = ['Rank', 'Sym', 'Price', 'Mcap', 'Vol 24h', '1 Hour', '24 Hour',
           '7 Day', '30 Days','3 Months', 'Amount', 'Value', '/100']

    # Build datarows
    for hold in db.portfolio.find():
        cursor = db.tickers_5t.find({"symbol":hold["symbol"]}
            ).sort("date",-1).limit(1)

        if cursor.count() < 1: continue
        tckr = cursor.next()

        value = round(hold['amount'] * ex * tckr['price_usd'], 2)
        profit += (tckr['pct_24h']/100) * value if tckr['pct_24h'] else 0.0
        total += value
        updated.append(tckr["date"].timestamp())

        datarows.append([
            tckr['rank'],
            tckr['symbol'],
            ex * round(tckr['price_usd'],2),
            ex * tckr.get('mktcap_usd',0),
            ex * tckr["vol_24h_usd"],
            tckr["pct_1h"],
            tckr["pct_24h"],
            tckr["pct_7d"],
            diff(tckr["symbol"], tckr["price_usd"], "30D", to_format="percentage"),
            diff(tckr["symbol"], tckr["price_usd"], "90D", to_format="percentage"),
            hold['amount'],
            value,
            None
        ])

    # Calculate porfolio %
    for datarow in datarows:
        datarow[12] = round((float(datarow[11]) / total)*100, 2)
    # Sort by value
    datarows = sorted(datarows, key=lambda x: int(x[11]))[::-1]

    rows, colors = [], []
    for datarow in datarows:
        colors.append(
            [c.WHITE]*5 + [pnlcolor(datarow[n]) for n in range(5,10)] + [c.WHITE]*3)
        rows.append([
            datarow[0],
            datarow[1],
            pretty(datarow[2], t='money'),
            pretty(datarow[3], t='money', abbr=True),
            pretty(datarow[4], t='money', abbr=True),
            pretty(datarow[5], t='pct', f='sign'),
            pretty(datarow[6], t='pct', f='sign'),
            pretty(datarow[7], t='pct', f='sign'),
            pretty(datarow[8], t='pct', f='sign'),
            pretty(datarow[9], t='pct', f='sign'),
            pretty(datarow[10], abbr=True),
            pretty(datarow[11], t='money'),
            pretty(datarow[12], t='pct')
        ])

    # Print title Row
    stdscr.clear()
    updated = to_relative_str(utc_datetime() - to_dt(max(updated))) + " ago"
    stdscr.addstr(0, 2, "Updated %s" % updated)
    stdscr.addstr(0, stdscr.getmaxyx()[1]-5, CURRENCY.upper())
    stdscr.addstr(1, 0, "")

    # Portfolio datatable
    print_table(stdscr, ["Portfolio"], hdr, rows, colors, align='right',colsp=2,div=True)
    stdscr.addstr(stdscr.getyx()[0]+1, 0, "")

    # Summary table
    print_table(
        stdscr,
        ["Summary"],
        ["Holdings", "24 Hour", "Total Value"],
        [[
            len(datarows),
            pretty(int(profit), t="money", f='sign', d=0),
            pretty(total, t='money')
        ]],
        [[c.WHITE, pnlcolor(profit), c.WHITE]],
        div=True)

    #log.debug("portfolio rendered in %s ms", t1)
