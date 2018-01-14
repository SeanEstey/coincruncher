
EXT_PATH = "%s/Library/Application Support/com.dmitrynikolaev.numi/extensions" % os.path.expanduser("~")

#----------------------------------------------------------------------
def export_numi(data):
    # Set numi "marketcap" global var
    buf = ''
    buf += "\nnumi.setVariable(\"marketcap\", { \"double\":%s, \"unitId\":\"%s\"});" %(
        markets['total_market_cap_cad'], currency)

    buf += "%s=%s;  " % (coin['js_name'], coin['price'])

    # Write data to extension
    try:
        file = open("%s/data.js" % EXT_PATH,"w")
        file.write(buf)
        file.close()
    except Exception as e:
        print_markets(markets)
        pass
    else:
        print_markets(markets)
