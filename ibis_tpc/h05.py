'Local Supplier Volume Query (Q5)'

import ibis

from .utils import add_date


def tpc_h05(con, NAME='ASIA', DATE='1994-01-01'):
    customer = con.table('customer')
    orders = con.table('orders')
    lineitem = con.table('lineitem')
    supplier = con.table('supplier')
    nation = con.table('nation')
    region = con.table('region')

    q = customer
    q = q.join(orders, customer.C_CUSTKEY == orders.O_CUSTKEY)
    q = q.join(lineitem, lineitem.L_ORDERKEY == orders.O_ORDERKEY)
    q = q.join(supplier, lineitem.L_SUPPKEY == supplier.S_SUPPKEY)
    q = q.join(nation, (customer.C_NATIONKEY == supplier.S_NATIONKEY) &
                       (supplier.S_NATIONKEY == nation.N_NATIONKEY))
    q = q.join(region, nation.N_REGIONKEY == region.R_REGIONKEY)
    q = q.materialize()

    q = q.filter([q.R_NAME == NAME,
                  q.O_ORDERDATE >= DATE,
                  q.O_ORDERDATE < add_date(DATE, dy=1)])
    revexpr = q.L_EXTENDEDPRICE*(1-q.L_DISCOUNT)
    gq = q.group_by([q.N_NAME])
    q = gq.aggregate(revenue=revexpr.sum())
    q = q.sort_by([ibis.desc(q.revenue)])
    return q
