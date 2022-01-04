
def tpc_h19(con, QUANTITY1=1, QUANTITY2=10, QUANTITY3=20,
            BRAND1='Brand#12', BRAND2='Brand#23', BRAND3='Brand#34'):
    '''Discounted Revenue Query (Q19)

    The Discounted Revenue Query reports the gross discounted revenue
    attributed to the sale of selected parts handled in a particular manner.
    This query is an example of code such as might be produced programmatically
    by a data mining tool.'''

    lineitem = con.table('lineitem')
    part = con.table('part')
    q = lineitem.join(part, part.P_PARTKEY == lineitem.L_PARTKEY)
    q = q.materialize()

    q1 = ((q.P_BRAND == BRAND1) &
          (q.P_CONTAINER.isin(('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))) &
          (q.L_QUANTITY >= QUANTITY1) &
          (q.L_QUANTITY <= QUANTITY1 + 10) &
          (q.P_SIZE.between(1, 5)) &
          (q.L_SHIPMODE.isin(('AIR', 'AIR REG'))) &
          (q.L_SHIPINSTRUCT == 'DELIVER IN PERSON'))

    q2 = ((q.P_BRAND == BRAND2) &
          (q.P_CONTAINER.isin(('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'))) &
          (q.L_QUANTITY >= QUANTITY2) &
          (q.L_QUANTITY <= QUANTITY2 + 10) &
          (q.P_SIZE.between(1, 10)) &
          (q.L_SHIPMODE.isin(('AIR', 'AIR REG'))) &
          (q.L_SHIPINSTRUCT == 'DELIVER IN PERSON'))

    q3 = ((q.P_BRAND == BRAND3) &
          (q.P_CONTAINER.isin(('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))) &
          (q.L_QUANTITY >= QUANTITY3) &
          (q.L_QUANTITY <= QUANTITY3 + 10) &
          (q.P_SIZE.between(1, 15)) &
          (q.L_SHIPMODE.isin(('AIR', 'AIR REG'))) &
          (q.L_SHIPINSTRUCT == 'DELIVER IN PERSON'))

    q = q.filter([q1 | q2 | q3])
    q = q.aggregate(revenue=(q.L_EXTENDEDPRICE*(1-q.L_DISCOUNT)).sum())
    return q
