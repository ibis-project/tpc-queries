import ibis


def tpc_h13(con, WORD1='special', WORD2='requests'):
    '''Customer Distribution Query (Q13)

       This query seeks relationships between customers and the size of their
       orders.'''

    customer = con.table('customer')
    orders = con.table('orders')
    innerq = customer
    innerq = innerq.left_join(orders, customer.C_CUSTKEY == orders.O_CUSTKEY)
    innerq = innerq.materialize()
    innerq = innerq.filter([
        ~innerq.O_COMMENT.like(f'%{WORD1}%{WORD2}%')
    ])
    innergq = innerq.group_by([innerq.C_CUSTKEY])
    innerq = innergq.aggregate(c_count=innerq.O_ORDERKEY.count())

    gq = innerq.group_by([innerq.c_count])
    q = gq.aggregate(custdist=innerq.c_count.count())

    q = q.sort_by([ibis.desc(q.custdist), ibis.desc(q.c_count)])
    return q
