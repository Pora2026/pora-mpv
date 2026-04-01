from datetime import date, timedelta


def is_sunday(d: date) -> bool:
    return d.weekday() == 6


def parse_ymd(s: str) -> date:
    y, m, d = map(int, s.split("-"))
    return date(y, m, d)


def iso(d: date) -> str:
    return d.isoformat()


def fmt_date_ar(d: date) -> str:
    return f"{d.day:02d}/{d.month:02d}/{d.year}"


def fmt_date_ar_from_iso(s: str) -> str:
    y, m, d = map(int, s.split("-"))
    return f"{d:02d}/{m:02d}/{y}"


def iter_dates(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)


def iter_workdays(d1: date, d2: date):
    for d in iter_dates(d1, d2):
        if not is_sunday(d):
            yield d


def month_range(d: date):
    first = d.replace(day=1)
    if d.month == 12:
        last = d.replace(day=31)
    else:
        nxt = d.replace(month=d.month + 1, day=1)
        last = nxt - timedelta(days=1)
    return first, last


def iter_month_labels(d1: date, d2: date):
    y, m = d1.year, d1.month
    end_y, end_m = d2.year, d2.month
    while (y < end_y) or (y == end_y and m <= end_m):
        yield f"{y}-{m:02d}"
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1