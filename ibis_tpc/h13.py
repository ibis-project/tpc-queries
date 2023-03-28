import ibis


def tpc_h13(con, WORD1="special", WORD2="requests"):
    """Customer Distribution Query (Q13)

    This query seeks relationships between customers and the size of their
    orders."""

    customer = con.table("customer")
    orders = con.table("orders")
    innerq = customer
    innerq = innerq.left_join(
        orders,
        (customer.c_custkey == orders.o_custkey)
        & ~orders.o_comment.like(f"%{WORD1}%{WORD2}%"),
    )
    innergq = innerq.group_by([innerq.c_custkey])
    innerq = innergq.aggregate(c_count=innerq.o_orderkey.count())

    gq = innerq.group_by([innerq.c_count])
    q = gq.aggregate(custdist=innerq.count())

    q = q.order_by([ibis.desc(q.custdist), ibis.desc(q.c_count)])
    return q
