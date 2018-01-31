import logging, os, pymongo
import db_auth
log = logging.getLogger(__name__)

#-------------------------------------------------------------------------------
def create_client(host=None, port=None, connect=True, auth=True):
    client = pymongo.MongoClient(
        host = host,
        port = port,
        tz_aware = True,
        connect = connect)

    if auth:
        authenticate(client)

    return client

#-------------------------------------------------------------------------------
def authenticate(client, user=None, pw=None):

    try:
        client.admin.authenticate(
            user or db_auth.user,
            pw or db_auth.password,
            mechanism='SCRAM-SHA-1')
    except Exception as e:
        log.exception("mongodb authentication error. host=%s, port=%s",
            client.HOST, client.PORT)
        raise

#-------------------------------------------------------------------------------
def dump(path):
    import os
    os.system("mongodump -o %s" % path)
    #log.info("MongoDB backup created")

#-------------------------------------------------------------------------------
def restore(path):
    import os
    os.system("mongorestore -d simbot %s" % path)

