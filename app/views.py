# app.views

import logging
import pandas as pd
from datetime import timedelta, datetime
from app import get_db, forex, markets, tickers
from app.utils import utc_dtdate, to_relative_str, to_int, to_dt, utc_datetime
from app.screen import c, print_table, pretty, pnlcolor
from app.timer import Timer
from config import CURRENCY
log = logging.getLogger('views')

#-----------------------------------------------------------------------------
def show_markets(stdscr):
    """Global market data.
    """
    _diff = markets.diff
    _to = pretty
    db = get_db()
    stdscr.clear()

    stdscr.addstr(0, 2, "Aggregate Market Data")
    stdscr.addstr(0, stdscr.getmaxyx()[1]-5, CURRENCY.upper())
    stdscr.addstr(1, 0, "")

    # Latest market (table)
    ex = forex.getrate('CAD',utc_dtdate())
    hdr = ['Mcap', '24h Vol', 'BTC Dominance', 'Markets', 'Currencies',
           '1 Hour', '24 Hour', '7 Day']
    mktdata = list(db.market_idx_5m.find().limit(1).sort('date',-1))
    if len(mktdata) == 0:
        return log.error("db.market_idx_5m empty")
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
            _to(_diff('7D', to_format='percentage'), t="pct", f="sign")
        ])
        colors.append([c.WHITE]*5 + [pnlcolor(rows[-1][col]) for col in range(5,8)])

    updated = to_relative_str(utc_datetime() - mktdata[0]["date"])
    print_table(
        stdscr,
        ["Latest (%s)" % updated],
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
    print_table(stdscr, ["Daily Historic"], hdr, rows, colors, div=True)

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
    log.debug("%s tickers queried in %sms",
        tickerdata.count(), t1.clock(t='ms'))

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
    db = get_db()
    ex = forex.getrate('CAD',utc_dtdate())
    rows, colors = [], []
    hdr = ["Rank", "Sym", "Price", "Market Cap", "24h Vol", "1 Hour", "24 Hour", "7d"]
    updated = []

    for watch in db.watchlist.find():
        cursor = db.tickers_5m.find({"symbol":watch["symbol"]}).sort("date",-1).limit(1)
        if cursor.count() < 1:
            continue
        tckr = cursor.next()
        rows.append([
            tckr["rank"],
            tckr["symbol"],
            pretty(ex * tckr["price_usd"], t='money'),
            pretty(ex * to_int(tckr["mktcap_usd"]), t='money', abbr=True),
            pretty(ex * to_int(tckr["vol_24h_usd"]), t='money', abbr=True),
            pretty(tckr["pct_1h"], t='pct', f='sign'),
            pretty(tckr["pct_24h"], t='pct', f='sign'),
            pretty(tckr["pct_7d"], t='pct', f='sign')
        ])
        colors.append(
            [c.WHITE]*5 +\
            [pnlcolor(rows[-1][5]), pnlcolor(rows[-1][6]), pnlcolor(rows[-1][7])])
        updated.append(tckr["date"].timestamp())

    rows = sorted(rows, key=lambda x: int(x[0]))

    # Print
    stdscr.clear()
    updated = to_relative_str(utc_datetime() - to_dt(max(updated)))
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
    datarows = []
    ex = forex.getrate('CAD',utc_dtdate())

    # Build datarows

    _tickers = list(db.tickers_5m.find().sort("date",-1))
    hdr = ['Rank', 'Sym', 'Price', 'Mcap', 'Vol 24h', '1 Hour', '24 Hour',
           '7 Day', '30 Day', 'Amount', 'Value', '/100']

    for hold in db.portfolio.find():
        for tckr in _tickers:
            if tckr['symbol'] != hold['symbol']:
                continue

            _30d = diff(tckr["symbol"], tckr["price_usd"], "30D",
                to_format="percentage")
            value = round(hold['amount'] * ex * tckr['price_usd'], 2)
            profit += (tckr['pct_24h']/100) * value if tckr['pct_24h'] else 0.0
            total += value

            datarows.append([
                tckr['rank'], tckr['symbol'], ex * round(tckr['price_usd'],2),
                ex * tckr.get('mktcap_usd',0), ex * tckr["vol_24h_usd"],
                tckr["pct_1h"], tckr["pct_24h"], tckr["pct_7d"], _30d,
                hold['amount'], value, None
            ])
            break

    # Calculate porfolio %
    for datarow in datarows:
        datarow[11] = round((float(datarow[10]) / total)*100, 2)
    # Sort by value
    datarows = sorted(datarows, key=lambda x: int(x[10]))[::-1]

    rows, colors = [], []
    for datarow in datarows:
        colors.append(
            [c.WHITE]*5 + [pnlcolor(datarow[n]) for n in range(5,9)] + [c.WHITE]*3)
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
            pretty(datarow[9], abbr=True),
            pretty(datarow[10], t='money'),
            pretty(datarow[11], t='pct')
        ])

    # Print title Row
    stdscr.clear()
    updated = to_relative_str(utc_datetime() - _tickers[0]["date"])
    stdscr.addstr(0, 2, "Updated %s" % updated)
    stdscr.addstr(0, stdscr.getmaxyx()[1]-5, CURRENCY.upper())
    stdscr.addstr(1, 0, "")

    # Portfolio datatable
    print_table(stdscr, ["Portfolio"], hdr, rows, colors, div=True)
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

    log.debug("portfolio rendered in %s ms", t1.clock(t='ms'))
