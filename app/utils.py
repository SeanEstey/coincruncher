import inspect, logging, re, unicodedata
from pprint import pformat
log = logging.getLogger(__name__)

# Datetime methods
import pytz
from datetime import datetime, timedelta, time, date
from dateutil import tz
from dateutil.parser import parse

#------------------------------------------------------------------------------
def to_ts(_datetime):
    return int(_datetime.timestamp())

#------------------------------------------------------------------------------
def utc_date():
    """current date in UTC timezone"""
    return datetime.utcnow().replace(tzinfo=pytz.utc).date()

#------------------------------------------------------------------------------
def utc_dtdate():
    """current date as datetime obj at T:00:00:00:00 in UTC timezone"""
    return datetime.combine(utc_date(), time()).replace(tzinfo=pytz.utc)

#------------------------------------------------------------------------------
def utc_datetime():
    """tz-aware UTC datetime object"""
    return datetime.utcnow().replace(tzinfo=pytz.utc)

#------------------------------------------------------------------------------
def duration(_timedelta, units='total_seconds'):
    if units == 'total_seconds':
        return int(_timedelta.total_seconds())
    elif units == 'hours':
        return round(_timedelta.total_seconds()/3600,1)

#------------------------------------------------------------------------------
def to_local(dt):
    return dt.astimezone(tz.tzlocal())

#------------------------------------------------------------------------------
def to_dt(val):
    """Convert timestamp or ISOstring to datetime obj
    """
    if val is None:
        return None
    elif type(val) == int:
        # Timestamp
        return datetime.utcfromtimestamp(val).replace(tzinfo=pytz.utc)
    elif type(val) == float:
        # Timestamp
        return datetime.utcfromtimestamp(val).replace(tzinfo=pytz.utc)
    elif type(val) == str:
        # Timestamp
        if re.match(r'^[0-9]*$', val):
            return datetime.utcfromtimestamp(float(val)).replace(tzinfo=pytz.utc)
        # ISO formatted datetime str?
        else:
            try:
                return parse(val).replace(tzinfo=pytz.utc)
            except Exception as e:
                raise
    raise Exception("to_dt(): invalid type '%s'" % type(val))

#----------------------------------------------------------------------
def to_relative_str(_delta):
    diff_ms = abs(_delta.total_seconds() * 1000)
    min_ms = 1000 * 60
    hour_ms = 1000 * 3600
    day_ms = hour_ms * 24
    week_ms = day_ms * 7
    month_ms = day_ms * 30
    year_ms = day_ms * 365

    if diff_ms >= year_ms:
        # Year(s) span
        nYears = int(diff_ms/year_ms)
        return "{} year{} ago".format(nYears, 's' if nYears > 1 else '')

    if diff_ms >= month_ms:
        # Month(s) span
        nMonths = int(diff_ms/month_ms)
        return "{} month{} ago".format(nMonths, 's' if nMonths > 1 else '')

    if diff_ms >= week_ms:
        # Week(s) span
        nWeeks = int(diff_ms/week_ms)
        return "{} week{} ago".format(nWeeks, 's' if nWeeks > 1 else '')

    if diff_ms >= day_ms:
        # Day(s) span
        nDays = int(diff_ms/day_ms)
        return "{} day{} ago".format(nDays, 's' if nDays > 1 else '')

    if diff_ms >= hour_ms:
        # Hour(s) span
        nHours = int(diff_ms/hour_ms)
        return "{} hour{} ago".format(nHours, 's' if nHours > 1 else '')

    if diff_ms >= min_ms:
        # Minute(s) span
        nMin = int(diff_ms/min_ms)
        return "{} min ago".format(nMin)

    # Second(s) span
    nSec = int(diff_ms/1000)
    return "{} second{} ago".format(nSec, 's' if nSec > 1 else '')

# Data type methods
import numpy

#------------------------------------------------------------------------------
def numpy_to_py(adict):
    """Convert dict containing numpy.int64 values to python int's
    """
    for k in adict:
        if type(adict[k]) == numpy.int64:
            adict[k] = int(adict[k])
        elif type(adict[k]) == numpy.float64:
            adict[k] = float(adict[k])
    return adict

#------------------------------------------------------------------------------
def to_int(val):
    if val is None:
        return 0
    elif type(val) is int:
        return val
    elif type(val) == str:
        if is_number(val):
            return int(float(val))
        else:
            return None
    elif type(val) == numpy.int64:
        return int(val)
    else:
        return int(val)

#------------------------------------------------------------------------------
def is_number(s):
    """ """
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
    """ """
    if val is None:
        return 0.0
    elif type(val) is float:
        return val
    elif not is_number(val):
        return None
    return round(float(val),dec) if dec else float(val)

# Miscellaneous methods

#------------------------------------------------------------------------------
def get_global_loggers():
    for key in logging.Logger.manager.loggerDict:
        print(key)
    print("--------")

#------------------------------------------------------------------------------
def parse_period(p):
    """Return properties tuple (quantity, time_unit, timedelta) from given time
    period string. Arg format: <int><time_unit>
    Examples: '1H' (1 hour), '1D' (24 hrs), '7D' (7 days)
    Return (quantity, time_unit) tuple
    """
    if type(p) != str:
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

#----------------------------------------------------------------------
def getAttributes(obj):
    result = ''
    for name, value in inspect.getmembers(obj):
        if callable(value) or name.startswith('__'):
            continue
        result += pformat("%s: %s" %(name, value)) + "\n"
    return result
