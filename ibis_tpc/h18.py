import ibis


def tpc_h18(con, QUANTITY=300):
    '''Large Volume Customer Query (Q18)

    The Large Volume Customer Query ranks customers based on their having
    placed a large quantity order. Large quantity orders are defined as those
    orders whose total quantity is above a certain level.'''

    customer = con.table('customer')
    orders = con.table('orders')
    lineitem = con.table('lineitem')

    subgq = lineitem.groupby([lineitem.L_ORDERKEY])
    subq = subgq.aggregate(qty_sum=lineitem.L_QUANTITY.sum())
    subq = subq.filter([subq.qty_sum > QUANTITY])

    q = customer
    q = q.join(orders, customer.C_CUSTKEY == orders.O_CUSTKEY)
    q = q.join(lineitem, orders.O_ORDERKEY == lineitem.L_ORDERKEY)
    q = q.materialize()
    q = q.filter([
        q.O_ORDERKEY.isin(subq.L_ORDERKEY)
    ])

    gq = q.groupby([q.C_NAME, q.C_CUSTKEY, q.O_ORDERKEY, q.O_ORDERDATE, q.O_TOTALPRICE])
    q = gq.aggregate(sum_qty=q.L_QUANTITY.sum())
    q = q.sort_by([ibis.desc(q.O_TOTALPRICE), q.O_ORDERDATE])
    return q.limit(100)
