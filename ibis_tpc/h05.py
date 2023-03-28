"Local Supplier Volume Query (Q5)"

import ibis

from .utils import add_date


def tpc_h05(con, NAME="ASIA", DATE="1994-01-01"):
    customer = con.table("customer")
    orders = con.table("orders")
    lineitem = con.table("lineitem")
    supplier = con.table("supplier")
    nation = con.table("nation")
    region = con.table("region")

    q = customer
    q = q.join(orders, customer.c_custkey == orders.o_custkey)
    q = q.join(lineitem, lineitem.l_orderkey == orders.o_orderkey)
    q = q.join(supplier, lineitem.l_suppkey == supplier.s_suppkey)
    q = q.join(
        nation,
        (customer.c_nationkey == supplier.s_nationkey)
        & (supplier.s_nationkey == nation.n_nationkey),
    )
    q = q.join(region, nation.n_regionkey == region.r_regionkey)

    q = q.filter(
        [q.r_name == NAME, q.o_orderdate >= DATE, q.o_orderdate < add_date(DATE, dy=1)]
    )
    revexpr = q.l_extendedprice * (1 - q.l_discount)
    gq = q.group_by([q.n_name])
    q = gq.aggregate(revenue=revexpr.sum())
    q = q.order_by([ibis.desc(q.revenue)])
    return q
