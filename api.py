import pycurl, requests, json, os, subprocess, sys
from pprint import pprint
from timer import Timer
from config import *

#----------------------------------------------------------------------
def get_markets():

    data=None
    try:
        r = requests.get("https://api.coinmarketcap.com/v1/global?convert=%s" % CURRENCY)
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
def get_tickers(db, start, limit):
    from io import BytesIO
    chunk_size = 100
    idx = start
    results = []
    t = Timer()

    c = pycurl.Curl()
    c.setopt(c.COOKIEFILE, '')
    c.setopt(c.VERBOSE, True)

    while idx < limit:
        uri = "https://api.coinmarketcap.com/v1/ticker/?start=%s&limit=%s&convert=%s" %(idx, chunk_size, CURRENCY)
        data=BytesIO()
        c.setopt(c.WRITEFUNCTION, data.write)
        c.setopt(c.URL, uri)
        c.perform()
        results += json.loads(data.getvalue().decode('utf-8'))
        idx += chunk_size

    print("Total time: %s sec" % t.clock())

    for ticker in results:
        result =db['tickers'].replace_one(
            {'symbol':ticker['symbol']},
            ticker,
            upsert=True
        )

    #print("\033[H\033[J")
    #print("Retrieved and stored %s items in %s sec" %(len(dictionary), t1.clock()))

#----------------------------------------------------------------------
def _get_ticker(ids, CURRENCY):
    """TODO: try this module:
    https://github.com/mrsmn/coinmarketcap
    """
    t1 = Timer()
    uri = "https://api.coinmarketcap.com/v1/ticker/%s/?convert=%s"
    c = pycurl.Curl()
    c.setopt(c.URL, uri %(ids[0],CURRENCY))
    c.setopt(c.COOKIEFILE, '')
    c.setopt(c.VERBOSE, True)
    c.perform()

    for _id in ids:
        c.setopt(c.URL, uri %(_id,CURRENCY))
        c.perform()

    print("\033[H\033[J")
    print("Finished in %s sec" % t1.clock())

    return True

#----------------------------------------------------------------------
def get_ticker(ids, CURRENCY):
    limit=75
    t1 = Timer()
    results = []

    for _id in ids:
        cmd = [
            "curl",
            "https://api.coinmarketcap.com/v1/ticker/%s/?convert=%s" %(_id, CURRENCY)
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
	    "https://api.coinmarketcap.com/v1/ticker/?convert=%s&limit=%s" %(CURRENCY, limit),
        "--verbose"
    ]

    for data in stream_req(cmds):
        buf += data
        sys.stdout.write(data) #, end="")

    os.system('cls' if os.name == 'nt' else 'clear')

    print("\nReceived %s results in %s s" %(limit, t1.clock()))

    return json.loads(buf)

    """try:
        r = requests.get("%s/ticker/%s/?convert=%s" %(COINCAP_BASE_URL, 'dotcoin', CURRENCY))
        data.append(json.loads(r.text)[0])
    except Exception as e:
        print("Request error for dotcoin")
    else:
        return data
    """
