import datetime


def add_date(datestr, dy=0, dm=0, dd=0):
    dt = datetime.date.fromisoformat(datestr)
    m = dt.month + dm
    y = dt.year + dy
    y += m // 12
    m = ((m - 1) % 12) + 1

    newdt = datetime.date(y, m, dt.day) + datetime.timedelta(days=dd)
    return newdt.isoformat()
