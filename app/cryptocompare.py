# app.cryptocompare

import logging, json, requests
from bs4 import BeautifulSoup
from pymongo import ReplaceOne
import pandas as pd
from pprint import pprint
from app import get_db
from app.timer import Timer
log = logging.getLogger('cryptocompare')

#------------------------------------------------------------------------------
def getcoins():
    db = get_db()
    t1 = Timer()
    url = "https://www.cryptocompare.com/api/data/coinlist/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    data = json.loads(soup.prettify())
    data = data['Data']

    log.debug("updating %s assets", len(data))

    ops=[]
    for k,v in data.items():
        ops.append(ReplaceOne({"Id":v["Id"]}, v, upsert=True))

    result = db.coins.bulk_write(ops)

    log.info("getcoins: mod=%s, upsert=%s (%s ms)",
        result.modified_count, result.upserted_count, t1.clock(t='ms'))
