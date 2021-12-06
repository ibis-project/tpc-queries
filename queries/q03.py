import ibis


def query_tpch_q03(con, MKTSEGMENT='BUILDING', DATE='1995-03-15'):
    customer = con.table('customer')
    orders = con.table('orders')
    lineitem = con.table('lineitem')

    q = customer.join(orders, customer.C_CUSTKEY == orders.O_CUSTKEY)
    q = q.join(lineitem, lineitem.L_ORDERKEY == orders.O_ORDERKEY).materialize()
    q = q.filter([
        q.C_MKTSEGMENT == MKTSEGMENT,
        q.O_ORDERDATE < DATE,
        q.L_SHIPDATE > DATE
    ])
    qg = q.group_by([q.L_ORDERKEY, q.O_ORDERDATE, q.O_SHIPPRIORITY])
    q = qg.aggregate(revenue=(q.L_EXTENDEDPRICE * (1 - q.L_DISCOUNT)).sum())
    q = q.sort_by([ibis.desc(q.revenue), q.O_ORDERDATE])
    q = q.limit(10)

    return q
