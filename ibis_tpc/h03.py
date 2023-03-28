"Shipping Priority Query (Q3)"


import ibis


def tpc_h03(con, MKTSEGMENT="BUILDING", DATE="1995-03-15"):
    customer = con.table("customer")
    orders = con.table("orders")
    lineitem = con.table("lineitem")

    q = customer.join(orders, customer.c_custkey == orders.o_custkey)
    q = q.join(lineitem, lineitem.l_orderkey == orders.o_orderkey)
    q = q.filter(
        [q.c_mktsegment == MKTSEGMENT, q.o_orderdate < DATE, q.l_shipdate > DATE]
    )
    qg = q.group_by([q.l_orderkey, q.o_orderdate, q.o_shippriority])
    q = qg.aggregate(revenue=(q.l_extendedprice * (1 - q.l_discount)).sum())
    q = q.order_by([ibis.desc(q.revenue), q.o_orderdate])
    q = q.limit(10)

    return q
