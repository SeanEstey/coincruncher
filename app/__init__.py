import logging
from . import mongo
from config import *
from client_config import *

log = logging.getLogger(__name__)

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



#---------------------------------------------------------------------------
def file_handler(level, file_path,
                 filtr=None, fmt=None, datefmt=None, color=None, name=None):

    from logging import DEBUG

    handler = logging.FileHandler(file_path)
    handler.setLevel(level)

    if name is not None:
        handler.name = name
    else:
        handler.name = 'lvl_%s_file_handler' % str(level or '')

    """if filtr == logging.DEBUG:
        handler.addFilter(DebugFilter())
    elif filtr == logging.INFO:
        handler.addFilter(InfoFilter())
    elif filtr == logging.WARNING:
        handler.addFilter(WarningFilter())
    """

    formatter = logging.Formatter(
        colors.BLUE + (fmt or '[%(asctime)s %(name)s %(threadName)s]: ' + colors.ENDC + color + '%(message)s') + colors.ENDC,
        #(datefmt or '%m-%d %H:%M'))
        (datefmt or '%H:%M:%S'))

    handler.setFormatter(formatter)
    return handler


logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
        file_handler(
            logging.DEBUG,
            DEBUGFILE,
            color=colors.WHITE
        ),
        file_handler(
            logging.INFO,
            LOGFILE,
            color=colors.WHITE
        )
    ]
)

client = mongo.create_client(host=MONGO_URL, port=MONGO_PORT, auth=True)
db = client[DB]
