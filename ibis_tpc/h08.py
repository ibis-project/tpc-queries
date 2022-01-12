'National Market Share Query (Q8)'

import ibis

from .utils import add_date


def tpc_h08(con,
                 NATION="BRAZIL",
                 REGION="AMERICA",
                 TYPE="ECONOMY ANODIZED STEEL",
                 DATE='1995-01-01'):
    part = con.table('part')
    supplier = con.table('supplier')
    lineitem = con.table('lineitem')
    orders = con.table('orders')
    customer = con.table('customer')
    region = con.table('region')
    n1 = con.table('nation')
    n2 = n1.view()

    q = part
    q = q.join(lineitem, part.P_PARTKEY == lineitem.L_PARTKEY)
    q = q.join(supplier, supplier.S_SUPPKEY == lineitem.L_SUPPKEY)
    q = q.join(orders, lineitem.L_ORDERKEY == orders.O_ORDERKEY)
    q = q.join(customer, orders.O_CUSTKEY == customer.C_CUSTKEY)
    q = q.join(n1, customer.C_NATIONKEY == n1.N_NATIONKEY)
    q = q.join(region, n1.N_REGIONKEY == region.R_REGIONKEY)
    q = q.join(n2, supplier.S_NATIONKEY == n2.N_NATIONKEY)

    q = q[
        orders.O_ORDERDATE.year().cast('string').name('o_year'),
        (lineitem.L_EXTENDEDPRICE*(1-lineitem.L_DISCOUNT)).name('volume'),
        n2.N_NAME.name('nation'),
        region.R_NAME,
        orders.O_ORDERDATE,
        part.P_TYPE,
    ]

    q = q.filter([
        q.R_NAME == REGION,
        q.O_ORDERDATE.between(DATE, add_date(DATE, dy=2, dd=-1)),
        q.P_TYPE == TYPE
    ])

    q = q.mutate(nation_volume=ibis.case().when(q.nation == NATION, q.volume).else_(0).end())
    gq = q.group_by([q.o_year])
    q = gq.aggregate(mkt_share=q.nation_volume.sum()/q.volume.sum())
    q = q.sort_by([q.o_year])
    return q
