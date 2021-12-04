import ibis


def query_tpch_q03(con, MKTSEGMENT='BUILDING', DATE='1995-03-15'):
    customer = con.table('customer')
    orders = con.table('orders')
    lineitem = con.table('lineitem')

    q = customer.join(orders, customer.C_CUSTKEY == orders.O_CUSTKEY)
    q = q.join(lineitem, lineitem.L_ORDERKEY == orders.O_ORDERKEY)
    q = q.filter([
        customer.C_MKTSEGMENT == MKTSEGMENT,
        orders.O_ORDERDATE < DATE,
        lineitem.L_SHIPDATE > DATE
    ])
    q = q.group_by([q.L_ORDERKEY, q.O_ORDERDATE, q.O_SHIPPRIORITY])
    revexpr = lineitem.L_EXTENDEDPRICE * (1-lineitem.L_DISCOUNT)
    q = q.aggregate(revenue=revexpr.sum())
    q = q.sort_by([ibis.desc(q.revenue), q.O_ORDERDATE])
    q = q.limit(10)

    return q
