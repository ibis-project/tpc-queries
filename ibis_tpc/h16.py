import ibis


ibis.options.sql.default_limit = 100000


def tpc_h16(con, BRAND='Brand#45', TYPE='MEDIUM POLISHED',
            SIZES=(49, 14, 23, 45, 19, 3, 36, 9)):
    '''Parts/Supplier Relationship Query (Q16)

       This query finds out how many suppliers can supply parts with given
       attributes. It might be used, for example, to determine whether there is
       a sufficient number of suppliers for heavily ordered parts.'''

    partsupp = con.table('partsupp')
    part = con.table('part')
    supplier = con.table('supplier')

    q = partsupp.join(part, part.P_PARTKEY == partsupp.PS_PARTKEY)
    q = q.materialize()
    q = q.filter([
        q.P_BRAND != BRAND,
        ~q.P_TYPE.like(f'{TYPE}%'),
        q.P_SIZE.isin(SIZES),
        ~q.PS_SUPPKEY.isin(supplier.filter([supplier.S_COMMENT.like('%Customer%Complaints%')]).S_SUPPKEY)
    ])
    gq = q.groupby([q.P_BRAND, q.P_TYPE, q.P_SIZE])
    q = gq.aggregate(supplier_cnt=q.PS_SUPPKEY.nunique())
    q = q.sort_by([ibis.desc(q.supplier_cnt), q.P_BRAND, q.P_TYPE, q.P_SIZE])
    return q
