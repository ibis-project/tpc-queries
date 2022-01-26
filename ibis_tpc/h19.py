
def tpc_h19(con, QUANTITY1=1, QUANTITY2=10, QUANTITY3=20,
            BRAND1='Brand#12', BRAND2='Brand#23', BRAND3='Brand#34'):
    '''Discounted Revenue Query (Q19)

    The Discounted Revenue Query reports the gross discounted revenue
    attributed to the sale of selected parts handled in a particular manner.
    This query is an example of code such as might be produced programmatically
    by a data mining tool.'''

    lineitem = con.table('lineitem')
    part = con.table('part')
    q = lineitem.join(part, part.p_partkey == lineitem.l_partkey)
    q = q.materialize()

    q1 = ((q.p_brand == BRAND1) &
          (q.p_container.isin(('sm case', 'sm box', 'sm pack', 'sm pkg'))) &
          (q.l_quantity >= QUANTITY1) &
          (q.l_quantity <= QUANTITY1 + 10) &
          (q.p_size.between(1, 5)) &
          (q.l_shipmode.isin(('air', 'air reg'))) &
          (q.l_shipinstruct == 'deliver in person'))

    q2 = ((q.p_brand == BRAND2) &
          (q.p_container.isin(('med bag', 'med box', 'med pkg', 'med pack'))) &
          (q.l_quantity >= QUANTITY2) &
          (q.l_quantity <= QUANTITY2 + 10) &
          (q.p_size.between(1, 10)) &
          (q.l_shipmode.isin(('air', 'air reg'))) &
          (q.l_shipinstruct == 'deliver in person'))

    q3 = ((q.p_brand == BRAND3) &
          (q.p_container.isin(('lg case', 'lg box', 'lg pack', 'lg pkg'))) &
          (q.l_quantity >= QUANTITY3) &
          (q.l_quantity <= QUANTITY3 + 10) &
          (q.p_size.between(1, 15)) &
          (q.l_shipmode.isin(('air', 'air reg'))) &
          (q.l_shipinstruct == 'deliver in person'))

    q = q.filter([q1 | q2 | q3])
    q = q.aggregate(revenue=(q.l_extendedprice*(1-q.l_discount)).sum())
    return q
