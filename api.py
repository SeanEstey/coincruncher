import pycurl, requests, json, os, subprocess, sys
from pprint import pprint
from timer import Timer


COINCAP_BASE_URL = "https://api.coinmarketcap.com/v1"
COINCAP_TICKER_URL = "https://api.coinmarketcap.com/v1/ticker/?" #convert=%s&limit=200"
WCI_API_KEY = "B8BDV74aQIoF5rQYgZNdQ8VBfdgPN0";
WCI_MARKETS_URL = "https://www.worldcoinindex.com/apiservice/getmarkets";
wci_uri = WCI_MARKETS_URL + "?key=" + WCI_API_KEY + "&fiat=cad";


#----------------------------------------------------------------------
def get_markets(currency):

    data=None
    try:
        r = requests.get("https://api.coinmarketcap.com/v1/global?convert=%s" % currency)
        data = json.loads(r.text)
    except Exception as e:
        return False
    else:
        return data

def subp(cmd):
    try:
        response = subprocess.check_output(cmd)
    except Exception as e:
        print("subprocess error: %s" % str(e))
    else:
        #print(response)
        return response

#----------------------------------------------------------------------
def stream_req(cmd):
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)

#----------------------------------------------------------------------
def get_tickers(limit, currency):
    # TODO: Divide limit into chunks of 100

    t1 = Timer()
    uri = "https://api.coinmarketcap.com/v1/ticker/?limit=%s&convert=%s" %(limit,currency)
    c = pycurl.Curl()
    c.setopt(c.URL, uri)
    c.setopt(c.COOKIEFILE, '')
    #c.setopt(c.VERBOSE, True)
    c.perform()

    print("\033[H\033[J")
    print("Finished in %s sec" % t1.clock())

#----------------------------------------------------------------------
def _get_ticker(ids, currency):
    """TODO: try this module:
    https://github.com/mrsmn/coinmarketcap
    """
    t1 = Timer()
    uri = "https://api.coinmarketcap.com/v1/ticker/%s/?convert=%s"
    c = pycurl.Curl()
    c.setopt(c.URL, uri %(ids[0],currency))
    c.setopt(c.COOKIEFILE, '')
    c.setopt(c.VERBOSE, True)
    c.perform()

    for _id in ids:
        c.setopt(c.URL, uri %(_id,currency))
        c.perform()

    print("\033[H\033[J")
    print("Finished in %s sec" % t1.clock())

    return True

#----------------------------------------------------------------------
def get_ticker(ids, currency):
    limit=75
    t1 = Timer()
    results = []

    for _id in ids:
        cmd = [
            "curl",
            "https://api.coinmarketcap.com/v1/ticker/%s/?convert=%s" %(_id, currency)
            #"--verbose"
        ]

        try:
            sys.stdout.write('\b'*80)
            response = json.loads(subprocess.check_output(cmd).decode('utf-8'))
            sys.stdout.write('\b'*80)
            #for data in stream_req(cmd):
            #    buf += data
            #buf = json.loads(subp(cmd))[0]
        except Exception as e:
            print("curl error: %s" % str(e))
            continue
        else:
            results.append(response[0])

    print("Received %s results in %s" %(len(results), t1.clock()))
    return results

    cmds = [
	    "curl",
	    "https://api.coinmarketcap.com/v1/ticker/?convert=%s&limit=%s" %(currency, limit),
        "--verbose"
    ]

    for data in stream_req(cmds):
        buf += data
        sys.stdout.write(data) #, end="")

    os.system('cls' if os.name == 'nt' else 'clear')

    print("\nReceived %s results in %s s" %(limit, t1.clock()))

    return json.loads(buf)

    """try:
        r = requests.get("%s/ticker/%s/?convert=%s" %(COINCAP_BASE_URL, 'dotcoin', currency))
        data.append(json.loads(r.text)[0])
    except Exception as e:
        print("Request error for dotcoin")
    else:
        return data
    """

#----------------------------------------------------------------------
def get_wci_markets():
    r = requests.get(wci_uri)
    r = json.loads(r.text)
    pprint(r)
