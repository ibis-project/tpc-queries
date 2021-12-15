'Volume Shipping Query (Q7)'

from utils import add_date


def tpc_h07(con, NATION1="FRANCE", NATION2="GERMANY", DATE='1995-01-01'):
    supplier = con.table('supplier')
    lineitem = con.table('lineitem')
    orders = con.table('orders')
    customer = con.table('customer')
    nation = con.table('nation')

    q = supplier
    q = q.join(lineitem, supplier.S_SUPPKEY == lineitem.L_SUPPKEY)
    q = q.join(orders, orders.O_ORDERKEY == lineitem.L_ORDERKEY)
    q = q.join(customer, customer.C_CUSTKEY == orders.O_CUSTKEY)
    n1 = nation
    n2 = nation.view()
    q = q.join(n1, supplier.S_NATIONKEY == n1.N_NATIONKEY)
    q = q.join(n2, customer.C_NATIONKEY == n2.N_NATIONKEY)

    q = q[
            n1.N_NAME.name("supp_nation"),
            n2.N_NAME.name("cust_nation"),
            lineitem.L_SHIPDATE,
            lineitem.L_EXTENDEDPRICE,
            lineitem.L_DISCOUNT,
            lineitem.L_SHIPDATE.year().cast('string').name('l_year'),
            (lineitem.L_EXTENDEDPRICE*(1-lineitem.L_DISCOUNT)).name('volume')
    ]

    q = q.filter([
        ((q.cust_nation == NATION1) & (q.supp_nation == NATION2)) |
        ((q.cust_nation == NATION2) & (q.supp_nation == NATION1)),
        q.L_SHIPDATE.between(DATE, add_date(DATE, dy=2, dd=-1))
        ])

    gq = q.group_by(['supp_nation', 'cust_nation', 'l_year'])
    q = gq.aggregate(revenue=q.volume.sum())
    q = q.sort_by(['supp_nation', 'cust_nation', 'l_year'])

    return q
