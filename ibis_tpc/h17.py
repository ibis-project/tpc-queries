
def tpc_h17(con, BRAND='Brand#23', CONTAINER='MED BOX'):
    '''Small-Quantity-Order Revenue Query (Q17)

    This query determines how much average yearly revenue would be lost if
    orders were no longer filled for small quantities of certain parts. This
    may reduce overhead expenses by concentrating sales on larger shipments.'''

    lineitem = con.table('lineitem')
    part = con.table('part')

    q = lineitem.join(part, part.P_PARTKEY == lineitem.L_PARTKEY)
    q = q.materialize()

    innerq = lineitem
    innerq = innerq[
        lineitem.L_EXTENDEDPRICE.name('extprice'),
        lineitem.L_QUANTITY
    ]
    innerq = innerq.filter([lineitem.L_PARTKEY == q.P_PARTKEY])

    q = q.filter([
        q.P_BRAND == BRAND,
        q.P_CONTAINER == CONTAINER,
        q.L_QUANTITY < (0.2*innerq.L_QUANTITY.mean())
    ])
    q = q.aggregate(avg_yearly=innerq.extprice.sum()/7.0)
    return q
