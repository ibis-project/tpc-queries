"Order Priority Checking Query (Q4)"

import ibis


def tpc_h04(con, DATE="1993-07-01"):
    orders = con.table("orders")
    lineitem = con.table("lineitem")
    cond = (lineitem.l_orderkey == orders.o_orderkey) & (
        lineitem.l_commitdate < lineitem.l_receiptdate
    )
    q = orders.filter(
        [
            cond.any(),
            orders.o_orderdate.cast("date") >= ibis.date(DATE),
            orders.o_orderdate.cast("date") < ibis.date(DATE)
            + ibis.interval(months=3),
        ]
    )
    q = q.group_by([orders.o_orderpriority])
    q = q.aggregate(order_count=orders.count())
    q = q.order_by([orders.o_orderpriority])
    return q
