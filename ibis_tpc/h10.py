"Returned Item Reporting Query (Q10)"

import ibis
from .utils import add_date


def tpc_h10(con, DATE="1993-10-01"):
    customer = con.table("customer")
    orders = con.table("orders")
    lineitem = con.table("lineitem")
    nation = con.table("nation")

    q = customer
    q = q.join(orders, customer.c_custkey == orders.o_custkey)
    q = q.join(lineitem, lineitem.l_orderkey == orders.o_orderkey)
    q = q.join(nation, customer.c_nationkey == nation.n_nationkey)

    q = q.filter(
        [
            (q.o_orderdate >= DATE) & (q.o_orderdate < add_date(DATE, dm=3)),
            q.l_returnflag == "R",
        ]
    )

    gq = q.group_by(
        [
            q.c_custkey,
            q.c_name,
            q.c_acctbal,
            q.c_phone,
            q.n_name,
            q.c_address,
            q.c_comment,
        ]
    )
    q = gq.aggregate(revenue=(q.l_extendedprice * (1 - q.l_discount)).sum())

    q = q.order_by(ibis.desc(q.revenue))
    return q.limit(20)
