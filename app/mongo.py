import logging, os, pymongo
from docs.mongo_key import *
from app.timer import Timer
log = logging.getLogger('mongodb')

#-------------------------------------------------------------------------------
def create_client(host=None, port=None, connect=True, auth=True):
    tmr = Timer()
    log.debug("connecting to \"%s:%s\"", host, port)

    client = pymongo.MongoClient(
        host = host,
        port = port,
        tz_aware = True,
        connect = connect)

    if auth:
        authenticate(client)

    log.debug("established connection (%sms)", tmr)
    return client

#-------------------------------------------------------------------------------
def authenticate(client, user=None, pw=None):
    try:
        client.admin.authenticate(
            user or DBUSER,
            pw or DBPASSWORD,
            mechanism='SCRAM-SHA-1')
    except Exception as e:
        log.exception("mongodb authentication error. host=%s, port=%s",
            client.HOST, client.PORT)
        raise

#-------------------------------------------------------------------------------
def locked():
    from app import client

    if not client.is_locked:
        return False

    msg = "mongoDB locked. Writes are blocked. Retrying in 1s"
    log.error(msg)
    time.sleep(1)

    if client.is_locked:
        log.error("mongoDB still locked after sleeping")
        return True
    else:
        return False

#-------------------------------------------------------------------------------
def dump(path):
    import os
    os.system("mongodump -o %s" % path)

#-------------------------------------------------------------------------------
def restore(path):
    import os
    os.system("mongorestore -d simbot %s" % path)

