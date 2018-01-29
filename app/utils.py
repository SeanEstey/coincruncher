import inspect
import unicodedata
from pprint import pformat

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
