from .utils import add_date


def tpc_h12(con, SHIPMODE1="MAIL", SHIPMODE2="SHIP", DATE="1994-01-01"):
    """'Shipping Modes and Order Priority Query (Q12)

    This query determines whether selecting less expensive modes of shipping is
    negatively affecting the critical-prior- ity orders by causing more parts
    to be received by customers after the committed date."""

    orders = con.table("orders")
    lineitem = con.table("lineitem")
    q = orders
    q = q.join(lineitem, orders.o_orderkey == lineitem.l_orderkey)

    q = q.filter(
        [
            q.l_shipmode.isin([SHIPMODE1, SHIPMODE2]),
            q.l_commitdate < q.l_receiptdate,
            q.l_shipdate < q.l_commitdate,
            q.l_receiptdate >= DATE,
            q.l_receiptdate < add_date(DATE, dy=1),
        ]
    )

    gq = q.group_by([q.l_shipmode])
    q = gq.aggregate(
        high_line_count=(
            q.o_orderpriority.case()
            .when("1-URGENT", 1)
            .when("2-HIGH", 1)
            .else_(0)
            .end()
        ).sum(),
        low_line_count=(
            q.o_orderpriority.case()
            .when("1-URGENT", 0)
            .when("2-HIGH", 0)
            .else_(1)
            .end()
        ).sum(),
    )
    q = q.order_by(q.l_shipmode)

    return q
