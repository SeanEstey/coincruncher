"""Grabs prices from Coinmarketcap API, writes them to Default.js file in Numi
extension directory.
Docs: https://coinmarketcap.com/api/
"""
import json, getopt, sys
from api import get_markets, get_ticker
from display import show_watchlist, show_markets, show_portfolio, show_spinner

freq = 30
currency = "CAD"

#----------------------------------------------------------------------
if __name__ == "__main__":
    try:
        argv = sys.argv[1:]
        opts, args = getopt.getopt(argv,"mwp", ['markets', 'watchlist', 'portfolio'])
    except getopt.GetoptError:
        sys.exit(2)

    user_data = json.load(open('data.json'))
    watchlist = user_data['watchlist']
    portfolio = user_data['portfolio']

    print("Updating prices in CAD every %ss...\n" % freq)

    while True:
        if set(['-w','--watchlist','-p','--portfolio']).intersection(set([n[0] for n in opts])):
            ticker_data = get_ticker()

        for opt, arg in opts:
            if opt in ('-m', '--markets'):
                market_data = get_markets(currency)
                show_markets(market_data)
            elif opt in ('-w', '--watchlist'):
                show_watchlist(watchlist, ticker_data)
            elif opt in ('-p', '--portfolio'):
                show_portfolio(portfolio, ticker_data)

        show_spinner(freq)
