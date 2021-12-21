'Returned Item Reporting Query (Q10)'

import ibis
from utils import add_date


def tpc_h10(con, DATE='1993-10-01'):
    customer = con.table('customer')
    orders = con.table('orders')
    lineitem = con.table('lineitem')
    nation = con.table('nation')

    q = customer
    q = q.join(orders, customer.C_CUSTKEY == orders.O_CUSTKEY)
    q = q.join(lineitem, lineitem.L_ORDERKEY == orders.O_ORDERKEY)
    q = q.join(nation, customer.C_NATIONKEY == nation.N_NATIONKEY)
    q = q.materialize()

    q = q.filter([
        (q.O_ORDERDATE >= DATE) & (q.O_ORDERDATE < add_date(DATE, dm=3)),
        q.L_RETURNFLAG == 'R',
    ])

    gq = q.group_by([
            q.C_CUSTKEY,
            q.C_NAME,
            q.C_ACCTBAL,
            q.C_PHONE,
            q.N_NAME,
            q.C_ADDRESS,
            q.C_COMMENT
    ])
    q = gq.aggregate(revenue=(q.L_EXTENDEDPRICE*(1-q.L_DISCOUNT)).sum())

    q = q.sort_by(ibis.desc(q.revenue))
    return q.limit(20)
