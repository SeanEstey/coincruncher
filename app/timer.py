'''app.lib.timer'''
import time, pytz
import dateparser
from datetime import datetime, time, timedelta
from app.utils import utc_datetime, utc_dtdate

class Timer():
    """Simple timer object which functions in one of 2
    modes: A) clock, counts time elapsed, or B) timer,
    counts time remaining until target datetime.
    """
    start = None
    expire = None
    name = None

    def __repr__(self):
        return str(self.elapsed())

    def __format__(self, format_spec):
        return "{:,}".format(self.elapsed())

    def set_expiry(self, target):
        """target can be datetime.datetime obj or recognizable
        string.
        """
        if isinstance(target, datetime):
            self.start = utc_datetime()
            self.expire = target
        elif isinstance(target, str):
            if target == "next hour change":
                self.start = utc_datetime()
                self.expire = datetime.combine(
                    utc_dtdate().date(),
                    time(utc_datetime().time().hour)
                ).replace(tzinfo=pytz.utc) + timedelta(hours=1)
            else:
                self.start = utc_datetime()
                self.expire = dateparser.parse(target)
        print("timer expiry set for %s" % self.expire)

    def reset(self):
        """
        """
        self.start = utc_datetime()

    def elapsed(self, unit='ms'):
        """
        """
        sec = (utc_datetime() - self.start).total_seconds()

        if unit == 'ms':
            return round(sec * 1000, 1)
        elif unit == 's':
            return round(sec, 1)

    def remain(self, unit='ms'):
        """If in timer mode, return time remaining as milliseconds
        integer (unit='ms') or minutes string (unit='str')
        """
        if self.expire is None:
            return None

        rem_ms = int((self.expire - utc_datetime()).total_seconds()*1000)

        if unit == 'ms':
            return rem_ms if rem_ms > 0 else 0
        elif unit == 'str':
            if rem_ms == 0:
                return "expired"
            else:
                return "%s min" % round((rem_ms/1000/3600)*60,1)

    def __init__(self, name=None, expire=None):
        """
        """
        self.start = utc_datetime()

        if name:
            self.name=name
        if expire:
            self.set_expiry(expire)
