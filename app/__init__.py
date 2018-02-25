# app
import logging
import textwrap
from logging import DEBUG, INFO, WARNING
from config import *
from app.utils import colors
log = logging.getLogger('app')

#---------------------------------------------------------------------------
def set_db(host):
    from .mongo import create_client
    global db, client
    client = create_client(
        host=host,
        port=27017,
        connect=True,
        auth=True)
    db = client[DB]
    return db

#---------------------------------------------------------------------------
def get_db():
    global db
    return db if db else log.error("DB host not set!")

#---------------------------------------------------------------------------
class DebugFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == DEBUG

#---------------------------------------------------------------------------
class InfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == INFO

#---------------------------------------------------------------------------
class WarningFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == WARNING

#---------------------------------------------------------------------------
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
def file_handler(level, path, filt=None):
    """Custom log handler.
    left-align "-8s" for 8 spaces on right, "8s" for left
    """
    handler = logging.FileHandler(path)
    handler.setLevel(level)
    handler.setFormatter(WrappedFixedIndentingLog(
        colors.BLUE+'[%(asctime)-3s,%(name)8s]: '+colors.ENDC+'%(message)s',
        '%b-%d %H:%M'
    ))
    handler.addFilter(DebugFilter()) if filt==DEBUG else\
    handler.addFilter(InfoFilter()) if filt==INFO else\
    handler.addFilter(WarningFilter()) if filt==WARNING else None
    return handler

#---------------------------------------------------------------------------

client = None
db = None
logging.basicConfig(
    level=DEBUG,
    handlers=[
        file_handler(DEBUG, DEBUGFILE, filt=DEBUG),
        file_handler(INFO, LOGFILE)
    ]
)
# STFU
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
