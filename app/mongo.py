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

#---------------------------------------------------------------------------
def bulk_write_capped(ops, coll):
    try:
        result = coll.bulk_write(ops)
    except Exception as e:
        log.exception("Bulk write error. %s", str(e))

        from app import get_db
        db = get_db()
        stats = db.command("collstats",coll.name)

        if stats['capped'] == False:
            return False

        max_size = stats['maxSize']

        # Capped collection full. Drop and re-create w/ indexes.
        if stats['size'] / max_size > 0.9:
            from pymongo import IndexModel, ASCENDING, DESCENDING

            log.info("Capped collection > 90% full. Dropping/recreating.")
            log.info("Manually recreate indexes: %s", list(coll.list_indexes()))
            name = coll.name
            coll.drop()

            db.create_collection(name, capped=True, size=max_size)
            # FIXME
            idx1 = IndexModel( [("symbol", ASCENDING)], name="symbol")
            idx2 = IndexModel( [("date", DESCENDING)], name="date_-1")
            db[name].create_indexes([idx1, idx2])

            log.info("Retrying bulk_write")
            try:
                result = db[name].bulk_write(ops)
            except Exception as e:
                log.exception("Error saving CMC tickers. %s", str(e))
                return False
    return True

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

