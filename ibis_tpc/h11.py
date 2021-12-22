import ibis


def tpc_h11(con, NATION='GERMANY', FRACTION=.0001):
    partsupp = con.table('partsupp')
    supplier = con.table('supplier')
    nation = con.table('nation')

    q = partsupp
    q = q.join(supplier, partsupp.PS_SUPPKEY == supplier.S_SUPPKEY)
    q = q.join(nation, nation.N_NATIONKEY == supplier.S_NATIONKEY)
    q = q.materialize()

    q = q.filter([q.N_NAME == NATION])

    innerq = partsupp
    innerq = innerq.join(supplier, partsupp.PS_SUPPKEY == supplier.S_SUPPKEY)
    innerq = innerq.join(nation, nation.N_NATIONKEY == supplier.S_NATIONKEY)
    innerq = innerq.materialize()
    innerq = innerq.filter([innerq.N_NAME == NATION])
    innerq = innerq.aggregate(total=(innerq.PS_SUPPLYCOST * innerq.PS_AVAILQTY).sum())

    gq = q.group_by([q.PS_PARTKEY])
    q = gq.aggregate(value=(q.PS_SUPPLYCOST * q.PS_AVAILQTY).sum())
    q = q.filter([q.value > innerq.total * FRACTION])
    q = q.sort_by(ibis.desc(q.value))
    return q
