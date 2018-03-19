'''app.lib.timer'''
import time, pytz
import dateparser
from datetime import datetime, time, timedelta
from app.utils import utc_datetime as now, utc_dtdate as today

class Timer():
    """Simple timer object which functions in one of 2
    modes: A) clock, counts time elapsed, or B) timer,
    counts time remaining until target datetime.
    """
    start = None
    expire = None
    name = None
    expire_str = None

    def __repr__(self):
        return str(self.elapsed())

    def __format__(self, format_spec):
        return "{:,}".format(self.elapsed())

    def set_expiry(self, target):
        """target can be datetime.datetime obj or recognizable
        string.
        """
        if isinstance(target, datetime):
            self.start = now()
            self.expire = target
        elif isinstance(target, str):
            if "every" in target:
                self.parts = target.split(" ")
                inc = int(self.parts[1])
                if self.parts[2] == 'clock':
                    # TODO: handle date wrapping edge case
                    unit = self.parts[3]
                    if unit in ['min', 'minute', 'minutes']:
                        solutions = [n for n in range(0,60) if n % inc == 0]
                        _nowt = now().time()
                        gt_min = list(filter(lambda x: (x > _nowt.minute), solutions))
                        if len(gt_min) == 0:
                            _time = time(_nowt.hour+1, solutions[0])
                        else:
                            _time = time(_nowt.hour, gt_min[0])
                        self.start = now()
                        self.expire = datetime.combine(today().date(), _time).replace(tzinfo=pytz.utc)
                        self.expire_str = target
                        print("expire=%s" % self.expire)
                        return True
                    else:
                        raise Exception("every '%s' unit not supported" % unit)
            if target == "next hour change":
                self.start = now()
                self.expire = datetime.combine(today().date(), time(now().time().hour)
                ).replace(tzinfo=pytz.utc) + timedelta(hours=1)
            else:
                self.start = now()
                self.expire = dateparser.parse(target)
        print("timer expiry set for %s" % self.expire)

    def reset(self):
        """
        """
        self.start = now()

        if self.expire_str:
            self.set_expiry(self.expire_str)

    def elapsed(self, unit='ms'):
        """
        """
        sec = (now() - self.start).total_seconds()

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

        rem_ms = int((self.expire - now()).total_seconds()*1000)

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
        self.start = now()

        if name:
            self.name=name
        if expire:
            self.set_expiry(expire)
