from .utils import add_date


def tpc_h12(con, SHIPMODE1='MAIL', SHIPMODE2='SHIP', DATE='1994-01-01'):
    ''''Shipping Modes and Order Priority Query (Q12)

    This query determines whether selecting less expensive modes of shipping is
    negatively affecting the critical-prior- ity orders by causing more parts
    to be received by customers after the committed date.'''

    orders = con.table('orders')
    lineitem = con.table('lineitem')
    q = orders
    q = q.join(lineitem, orders.O_ORDERKEY == lineitem.L_ORDERKEY)
    q = q.materialize()

    q = q.filter([
        q.L_SHIPMODE.isin([SHIPMODE1, SHIPMODE2]),
        q.L_COMMITDATE < q.L_RECEIPTDATE,
        q.L_SHIPDATE < q.L_COMMITDATE,
        q.L_RECEIPTDATE >= DATE,
        q.L_RECEIPTDATE < add_date(DATE, dy=1),
    ])

    gq = q.group_by([q.L_SHIPMODE])
    q = gq.aggregate(
            high_line_count=(q.O_ORDERPRIORITY.case().when('1-URGENT', 1).when('2-HIGH', 1).else_(0).end()).sum(),
            low_line_count=(q.O_ORDERPRIORITY.case().when('1-URGENT', 0).when('2-HIGH', 0).else_(1).end()).sum())
    q = q.sort_by(q.L_SHIPMODE)

    return q
