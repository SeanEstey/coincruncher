# app
import logging
import textwrap
from logging import DEBUG, ERROR, INFO, WARNING, CRITICAL
from docs.config import *
from app.utils import colors
logging.addLevelName(SIGNAL, "Signal")
log = logging.getLogger('app')

class DebugFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == DEBUG

class InfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == INFO

class ErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == ERROR

class CriticalFilter(logging.Filter):
    def filter(self, record):
        return record.levelno <= CRITICAL

class SignalFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == SIGNAL

class WarningFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == WARNING

class WrappedFixedIndentingLog(logging.Formatter):
    def __init__(self,
                 fmt=None,
                 datefmt=None,
                 style='%',
                 width=MAX_LOG_LINE_WIDTH,
                 indent=LOG_NEWL_INDENT):
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)
        self.wrapper = textwrap.TextWrapper(
            width=width,
            subsequent_indent=' '*indent)

    def format(self, record):
        return self.wrapper.fill(super().format(record))

#---------------------------------------------------------------------------
def file_handler(level, path, filters=None):
    """Custom log handler.
    left-align "-8s" for 8 spaces on right, "8s" for left
    """
    handler = logging.FileHandler(path)
    handler.setLevel(level)
    handler.setFormatter(WrappedFixedIndentingLog(
        colors.BLUE+'[%(asctime)-3s,%(name)8s]: '+colors.ENDC+'%(message)s',
        '%m-%d %H:%M'
    ))
    if filters is None:
        return handler

    for _filter in filters:
        if _filter==DEBUG:
            handler.addFilter(DebugFilter())
        elif _filter==INFO:
            handler.addFilter(InfoFilter())
        elif _filter==WARNING:
            handler.addFilter(WarningFilter())
        elif _filter==ERROR:
            handler.addFilter(ErrorFilter())
        elif _filter==CRITICAL:
            handler.addFilter(CriticalFilter())
        elif _filter==SIGNAL:
            handler.addFilter(SignalFilter())
    return handler


#---------------------------------------------------------------------------
def set_db(host):
    from .mongo import create_client
    global db, client
    client = create_client(
        host=host,
        port=27017,
        connect=True,
        auth=True)
    db = client[DB_NAME]
    return db

#---------------------------------------------------------------------------
def get_db():
    global db
    return db if db else log.error("DB host not set!")

# Globals
client = None
db = None
logging.basicConfig(
    level=DEBUG,
    handlers=[
        file_handler(DEBUG, DEBUGFILE, filters=[DEBUG]),
        file_handler(INFO, LOGFILE, filters=[CRITICAL]),
        file_handler(SIGNAL, SIGNALFILE, filters=[SIGNAL])
    ]
)

# STFU
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
