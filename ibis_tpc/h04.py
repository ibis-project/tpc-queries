"Order Priority Checking Query (Q4)"

from .utils import add_date


def tpc_h04(con, DATE="1993-07-01"):
    orders = con.table("orders")
    lineitem = con.table("lineitem")
    cond = (lineitem.l_orderkey == orders.o_orderkey) & (
        lineitem.l_commitdate < lineitem.l_receiptdate
    )
    q = orders.filter(
        [
            cond.any(),
            orders.o_orderdate >= DATE,
            orders.o_orderdate < add_date(DATE, dm=3),
        ]
    )
    q = q.group_by([orders.o_orderpriority])
    q = q.aggregate(order_count=orders.count())
    q = q.order_by([orders.o_orderpriority])
    return q
