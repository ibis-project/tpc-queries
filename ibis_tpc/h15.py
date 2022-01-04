from .utils import add_date


def tpc_h15(con, DATE='1996-01-01'):
    '''Top Supplier Query (Q15)

        This query determines the top supplier so it can be rewarded, given
        more business, or identified for special recognition.'''

    lineitem = con.table('lineitem')
    supplier = con.table('supplier')

    qrev = lineitem
    qrev = qrev.filter([
        lineitem.L_SHIPDATE >= DATE,
        lineitem.L_SHIPDATE < add_date(DATE, dm=3)
    ])

    gqrev = qrev.group_by([lineitem.L_SUPPKEY])
    qrev = gqrev.aggregate(total_revenue=(qrev.L_EXTENDEDPRICE*(1-qrev.L_DISCOUNT)).sum())

    q = supplier.join(qrev, supplier.S_SUPPKEY == qrev.L_SUPPKEY)
    q = q.materialize()
    q = q.filter([
        q.total_revenue == qrev.total_revenue.sum()
    ])
    q = q.sort_by([q.S_SUPPKEY])
    return q
