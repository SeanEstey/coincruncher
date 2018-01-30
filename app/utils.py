import inspect, logging, unicodedata
from datetime import timedelta
from pprint import pformat
log = logging.getLogger(__name__)

#------------------------------------------------------------------------------
def parse_period(p):
    """Return properties tuple (quantity, time_unit, timedelta) from given time
    period string. Arg format: <int><time_unit>
    Examples: '1H' (1 hour), '1D' (24 hrs), '7D' (7 days)
    Return (quantity, time_unit) tuple
    """
    if type(p) != string:
        log.error("period '%s' must be a string, not %s", p, type(p))
        raise TypeError

    qty = int(p[0:-1]) if len(p) > 1 else 1
    unit = p[-1]

    if unit == 'M':
        tdelta = timedelta(minutes = qty)
    elif unit == 'H':
        tdelta = timedelta(hours = qty)
    elif unit == 'D':
        tdelta = timedelta(days = qty)
    elif unit == 'Y':
        tdelta = timedelta(days = 365 * qty)

    return (qty, unit, tdelta)

#------------------------------------------------------------------------------
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
    try:
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
    return False

#------------------------------------------------------------------------------
def to_float(val, dec=None):
    if not is_number(val):
        return None
    return round(float(val),dec) if dec else float(val)

#----------------------------------------------------------------------
def getAttributes(obj):
    result = ''
    for name, value in inspect.getmembers(obj):
        if callable(value) or name.startswith('__'):
            continue
        result += pformat("%s: %s" %(name, value)) + "\n"
    return result
