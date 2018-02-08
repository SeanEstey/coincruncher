import logging
from logging import DEBUG, INFO, WARNING
from .mongo import create_client, authenticate
from config import *

logging.getLogger("requests").setLevel(logging.ERROR)
log = logging.getLogger('app')

class colors:
    BLUE = '\033[94m'
    GRN = '\033[92m'
    YLLW = '\033[93m'
    RED = '\033[91m'
    WHITE = '\033[37m'
    ENDC = '\033[0m'
    HEADER = '\033[95m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
class DebugFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == DEBUG
class InfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == INFO
class WarningFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == WARNING

#---------------------------------------------------------------------------
def set_db(host):
    global db, client
    client = create_client(host=host, port=27017, connect=True, auth=True)
    db = client[DB]
    return db

#---------------------------------------------------------------------------
def get_db():
    global db
    if db:
        return db
    else:
        log.error("db host not set!")
        return None

#---------------------------------------------------------------------------
def file_handler(level, file_path,
                 filt=None, fmt=None, datefmt=None, color=None, name=None):
    handler = logging.FileHandler(file_path)
    handler.setLevel(level)

    if name is not None:
        handler.name = name
    else:
        handler.name = 'lvl_%s_file_handler' % str(level or '')

    if filt == DEBUG:
        handler.addFilter(DebugFilter())
    elif filt == INFO:
        handler.addFilter(InfoFilter())
    elif filt == WARNING:
        handler.addFilter(WarningFilter())

    # To show thread: %(threadName)s
    formatter = logging.Formatter(
        colors.BLUE + (fmt or '%(asctime)s %(name)s: '\
        + colors.ENDC + color + '%(message)s') + colors.ENDC,
        (datefmt or '%m-%d %H:%M:%S'))
    handler.setFormatter(formatter)
    return handler


logging.basicConfig(level=DEBUG, handlers=[
    file_handler(DEBUG, DEBUGFILE, filt=DEBUG, color=colors.WHITE),
    file_handler(INFO, LOGFILE, color=colors.WHITE)
    #file_handler(WARNING, LOGFILE, filt=WARNING, color=colors.WHITE),
    #file_handler(ERROR, LOGFILE, filt=WARNING, color=colors.WHITE),
])
client = None
db = None
