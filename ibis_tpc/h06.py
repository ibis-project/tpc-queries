"Forecasting Revenue Change Query (Q6)"

from .utils import add_date


def tpc_h06(con, DATE="1994-01-01", DISCOUNT=0.06, QUANTITY=24):
    lineitem = con.table("lineitem")

    tables = (lineitem,)

    return _tpc_h06(tables, DATE, DISCOUNT, QUANTITY)


def _tpc_h06(tables, DATE="1994-01-01", DISCOUNT=0.06, QUANTITY=24):

    lineitem, = tables

    discount_min = round(DISCOUNT - 0.01, 2)
    discount_max = round(DISCOUNT + 0.01, 2)
    q = lineitem.filter(
        [
            lineitem.l_shipdate >= DATE,
            lineitem.l_shipdate < add_date(DATE, dy=1),
            lineitem.l_discount.between(discount_min, discount_max),
            lineitem.l_quantity < QUANTITY,
        ]
    )
    q = q.aggregate(revenue=(q.l_extendedprice * q.l_discount).sum())
    return q
