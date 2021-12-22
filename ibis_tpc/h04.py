'Order Priority Checking Query (Q4)'

from .utils import add_date


def tpc_h04(con, DATE='1993-07-01'):
    orders = con.table('orders')
    lineitem = con.table('lineitem')
    cond = ((lineitem.L_ORDERKEY == orders.O_ORDERKEY) &
            (lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE))
    q = orders.filter([cond.any(),
                       orders.O_ORDERDATE >= DATE,
                       orders.O_ORDERDATE < add_date(DATE, dm=3)])
    q = q.group_by([orders.O_ORDERPRIORITY])
    q = q.aggregate(order_count=orders.count())
    q = q.sort_by([orders.O_ORDERPRIORITY])
    return q
