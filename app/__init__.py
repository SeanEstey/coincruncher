# app
import logging
import signal as libsignal
import textwrap
from docs.conf import *
from logging import DEBUG, ERROR, INFO, WARNING, CRITICAL
from app.common.utils import colors

logging.addLevelName(SIGNAL, "Signal")
logging.addLevelName(TRADE, "Trade")
logging.addLevelName(SCAN, "Scan")
log = logging.getLogger('app')

# Frequency in seconds to str
freqtostr = {
    60: "1m",
    300: "5m",
    1800: "30m",
    3600: "1h",
    86400: "1d"
}
# Period in seconds to str
pertostr = {
    1800:"30m",
    3600:"60m",
    7200:"120m",
    10800: "180m",
    86400:"24h",
    172800:"48h",
    259200:"72h",
    604800:"7d",
    1209600:"14d",
    1814400:"21d"
}
strtofreq = dict(zip(list(freqtostr.values()), list(freqtostr.keys())))
strtoper = dict(zip(list(pertostr.values()), list(pertostr.keys())))

#---------------------------------------------------------------------------
class GracefulKiller:
    kill_now = False
    def __init__(self):
        """
        """
        libsignal.signal(libsignal.SIGINT, self.exit_gracefully)
        libsignal.signal(libsignal.SIGTERM, self.exit_gracefully)
    def exit_gracefully(self,signum, frame):
        """
        """
        self.kill_now = True

#---------------------------------------------------------------------------
class WrappedFixedIndentingLog(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%',
        width=max_log_line_width, indent=log_newl_indent):
        """
        """
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)
        self.wrapper = textwrap.TextWrapper(
            width=width,
            subsequent_indent=' '*indent)
    def format(self, record):
        """
        """
        return self.wrapper.fill(super().format(record))

class DebugFilter(logging.Filter):
    def filter(self, record): return record.levelno == DEBUG

class InfoFilter(logging.Filter):
    def filter(self, record): return record.levelno == INFO

class ErrorFilter(logging.Filter):
    def filter(self, record): return record.levelno == ERROR

class CriticalFilter(logging.Filter):
    def filter(self, record): return record.levelno <= CRITICAL

class SignalFilter(logging.Filter):
    def filter(self, record): return record.levelno == SIGNAL

class TradeFilter(logging.Filter):
    def filter(self, record): return record.levelno == TRADE

class ScanFilter(logging.Filter):
    def filter(self, record): return record.levelno == SCAN

class WarningFilter(logging.Filter):
    def filter(self, record): return record.levelno == WARNING

#---------------------------------------------------------------------------
def eod_tasks():
    import os
    from docs.mongo_key import DBUSER, DBPASSWORD, AUTHDB
    from app.common import forex

    #forex.update_1d()

    log.debug('running mongodump...')

    os.system('mongodump -u %s -p %s -d coincruncher -o ~/Dropbox/mongodumps \
        --authenticationDatabase %s' %(DBUSER, DBPASSWORD, AUTHDB))
    log.info('eod tasks completed')

#---------------------------------------------------------------------------
def file_handler(level, path, filters=None):
    """Custom log handler.
    left-align "-8s" for 8 spaces on right, "8s" for left
    """
    handler = logging.FileHandler(path)
    handler.setLevel(level)

    std = WrappedFixedIndentingLog(
        colors.BLUE+'[%(asctime)-3s,%(name)8s]: '+colors.ENDC+'%(message)s',
        '%m-%d %H:%M'
    )

    if filters is None:
        return handler

    for _filter in filters:
        if _filter==DEBUG:
            handler.setFormatter(std)
            handler.addFilter(DebugFilter())
        elif _filter==INFO:
            handler.setFormatter(std)
            handler.addFilter(InfoFilter())
        elif _filter==WARNING:
            handler.setFormatter(std)
            handler.addFilter(WarningFilter())
        elif _filter==ERROR:
            handler.setFormatter(std)
            handler.addFilter(ErrorFilter())
        elif _filter==CRITICAL:
            handler.setFormatter(std)
            handler.addFilter(CriticalFilter())
        elif _filter==SIGNAL:
            short = WrappedFixedIndentingLog(
                colors.BLUE+'[%(asctime)-3s, signal]: '+colors.ENDC+'%(message)s',
                datefmt = '%H:%M:%S',
                width = 200
            )
            handler.setFormatter(short)
            handler.addFilter(SignalFilter())
        elif _filter==TRADE:
            short = WrappedFixedIndentingLog(
                colors.BLUE+'[%(asctime)-3s, trade]: '+colors.ENDC+'%(message)s',
                '%H:%M:%S')
            handler.setFormatter(short)
            handler.addFilter(TradeFilter())
        elif _filter==SCAN:
            short = WrappedFixedIndentingLog(
                fmt = colors.BLUE+'[%(asctime)-3s, trade]: '+colors.ENDC+'%(message)s',
                datefmt = '%H:%M:%S',
                width = 300
            )
            handler.setFormatter(short)
            handler.addFilter(ScanFilter())
    return handler


#---------------------------------------------------------------------------
def set_db(host):
    from app.common.mongo import create_client
    global db, client
    client = create_client(
        host=host,
        port=27017,
        connect=True,
        auth=True)
    db = client[db_name]
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
        file_handler(DEBUG, debugfile, filters=[DEBUG]),
        file_handler(INFO, logfile, filters=[CRITICAL]),
        file_handler(SIGNAL, signalfile, filters=[SIGNAL]),
        file_handler(TRADE, tradefile, filters=[TRADE]),
        file_handler(SCAN, scannerfile, filters=[SCAN])
    ]
)

def keystostr(keys): return (keys[0], freqtostr[keys[1]], pertostr[keys[2]])

# STFU
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)


