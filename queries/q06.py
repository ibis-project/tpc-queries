'Forecasting Revenue Change Query (Q6)'

from utils import add_date


def query_tpch_q06(con, DATE='1994-01-01', DISCOUNT=0.06, QUANTITY=24):
    q = con.table('lineitem')
    discount_min = round(DISCOUNT-.01, 2)
    discount_max = round(DISCOUNT+.01, 2)
    q = q.filter([
                    q.L_SHIPDATE >= DATE,
                    q.L_SHIPDATE < add_date(DATE, dy=1),
                    q.L_DISCOUNT.between(discount_min, discount_max),
                    q.L_QUANTITY < QUANTITY
                    ])
    q = q.aggregate(revenue=(q.L_EXTENDEDPRICE*q.L_DISCOUNT).sum())
    return q
