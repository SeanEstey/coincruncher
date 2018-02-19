'''app.lib.timer'''
import time
from datetime import datetime
from app.utils import utc_datetime

class Timer():
    start = None
    expire = None

    def __repr__(self):
        return str(self.elapsed())
    def __format__(self, format_spec):
        return "{:,}".format(self.elapsed())
    def set_expiry(self, dt):
        self.start = utc_datetime()
        self.expire = dt
    def reset(self):
        self.start = utc_datetime()
    def elapsed(self):
        return int((utc_datetime() - self.start).total_seconds()*1000)
    def remaining(self):
        if self.expire is None:
            return None
        rem = int((self.expire - utc_datetime()).total_seconds()*1000)
        return rem if rem > 0 else 0
    def __init__(self, expire=None):
        self.start = utc_datetime()
        if expire:
            self.expire = expire
