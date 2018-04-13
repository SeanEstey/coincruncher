""" app.common.timeutils
"""

#---------------------------------------------------------------------------
def freqtostr(sec, fmt='std'):
    """Pandas aliases:
    Alias   Description
    D       calendar day frequency
    W       weekly frequency
    H       hourly frequency
    T, min  minutely frequency
    S       secondly frequency
    L, ms   milliseonds
    U, us   microseconds
    N       nanoseconds
    """
    freqmap = {
        'minutes': {
            'std': 'm',
            'pandas': 'min'
        },
        'hours': {
            'std': 'h',
            'pandas': 'H'
        },
        'days': {
            'std': 'd',
            'pandas': 'D'
        },
        'weeks': {
            'std': 'w',
            'pandas': 'W'
        }
    }
    key = 'std' if fmt == 'std' else 'pandas'

    if sec < 3600:
        return "{}{}".format(int(sec/60), freqmap['minutes'][key])
    elif sec >= 3600 and sec < 86400:
        return "{}{}".format(int(sec/3600), freqmap['hours'][key])
    elif sec >= 86400 and sec < 604800:
        return "{}{}".format(int(sec/86400), freqmap['days'][key])
    elif sec >= 604800:
        return "{}{}".format(int(sec/604800), freqmap['weeks'][key])

#------------------------------------------------------------------------------
def strtofreq(freqstr):
    """Convert frequency string to seconds.
    :param freqstr: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
    """
    sec_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }
    sec = None
    unit = freqstr[-1]
    if unit in sec_per_unit:
        try:
            sec = int(freqstr[:-1]) * sec_per_unit[unit]
        except ValueError:
            pass
    return sec
