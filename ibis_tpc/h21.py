import ibis


def tpc_h21(con, NATION='SAUDI ARABIA'):
    '''Suppliers Who Kept Orders Waiting Query (Q21)

    This query identifies certain suppliers who were not able to ship required
    parts in a timely manner.'''

    supplier = con.table('supplier')
    lineitem = con.table('lineitem')
    orders = con.table('orders')
    nation = con.table('nation')

    L2 = lineitem.view()
    L3 = lineitem.view()

    q = supplier
    q = q.join(lineitem, supplier.S_SUPPKEY == lineitem.L_SUPPKEY)
    q = q.join(orders, orders.O_ORDERKEY == lineitem.L_ORDERKEY)
    q = q.join(nation, supplier.S_NATIONKEY == nation.N_NATIONKEY)
    q = q.materialize()
    q = q[
        q.L_ORDERKEY.name("L1_ORDERKEY"),
        q.O_ORDERSTATUS,
        q.L_RECEIPTDATE,
        q.L_COMMITDATE,
        q.L_SUPPKEY.name("L1_SUPPKEY"),
        q.S_NAME,
        q.N_NAME,
    ]
    q = q.filter([
        q.O_ORDERSTATUS == 'F',
        q.L_RECEIPTDATE > q.L_COMMITDATE,
        q.N_NAME == NATION,
        ((L2.L_ORDERKEY == q.L1_ORDERKEY) & (L2.L_SUPPKEY != q.L1_SUPPKEY)).any(),
        ~(((L3.L_ORDERKEY == q.L1_ORDERKEY) & (L3.L_SUPPKEY != q.L1_SUPPKEY) & (L3.L_RECEIPTDATE > L3.L_COMMITDATE)).any()),
    ])

    gq = q.group_by([q.S_NAME])
    q = gq.aggregate(numwait=q.count())
    q = q.sort_by([ibis.desc(q.numwait), q.S_NAME])
    return q.limit(100)
