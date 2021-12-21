'Product Type Profit Measure Query (Q9)'

import ibis


def tpc_h09(con, COLOR='green'):
    part = con.table('part')
    supplier = con.table('supplier')
    lineitem = con.table('lineitem')
    partsupp = con.table('partsupp')
    orders = con.table('orders')
    nation = con.table('nation')

    q = lineitem
    q = q.join(supplier, supplier.S_SUPPKEY == lineitem.L_SUPPKEY)
    q = q.join(partsupp, (partsupp.PS_SUPPKEY == lineitem.L_SUPPKEY) &
                         (partsupp.PS_PARTKEY == lineitem.L_PARTKEY))
    q = q.join(part, part.P_PARTKEY == lineitem.L_PARTKEY)
    q = q.join(orders, orders.O_ORDERKEY == lineitem.L_ORDERKEY)
    q = q.join(nation, supplier.S_NATIONKEY == nation.N_NATIONKEY)
    q = q.materialize()

    q = q[
        (q.L_EXTENDEDPRICE*(1-q.L_DISCOUNT)-q.PS_SUPPLYCOST*q.L_QUANTITY).name('amount'),
        q.O_ORDERDATE.year().cast('string').name('o_year'),
        q.N_NAME.name('nation'),
        q.P_NAME
    ]

    q = q.filter([q.P_NAME.like('%'+COLOR+'%')])

    gq = q.group_by([q.nation, q.o_year])
    q = gq.aggregate(sum_profit=q.amount.sum())
    q = q.sort_by([q.nation, ibis.desc(q.o_year)])
    return q
