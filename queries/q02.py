def tpch_q02(con, REGION='EUROPE', SIZE=15, TYPE='BRASS'):
    '''The Minimum Cost Supplier Query finds, in a given region, for each part
    of a certain type and size, the supplier who can  supply  it  at  minimum
    cost.  If  several  suppliers  in  that  region  offer  the  desired  part
    type  and  size  at  the  same (minimum)  cost,  the  query  lists  the
    parts  from  suppliers  with  the  100  highest  account  balances.  For
    each  supplier, the  query  lists  the  supplier's  account  balance,  name
    and  nation;  the  part's  number  and  manufacturer;  the  supplier's
    address, phone number and comment information. '''

    part = con.table('part')
    partsupp = con.table('partsupp')
    supplier = con.table('supplier')
    nation = con.table('nation')
    region = con.table('region')

    q = partsupp.join(supplier, supplier.S_SUPPKEY == partsupp.PS_SUPPKEY)
    q = q.join(nation, supplier.S_NATIONKEY == nation.N_NATIONKEY)
    q = q.join(region, [nation.N_REGIONKEY == region.R_REGIONKEY, region.R_NAME == REGION])
#    q = q.materialize()
#    q = q.filter()
#    q = q.mutate(supplyfloat = q.PS_SUPPLYCOST.cast('double'))
    q = q.aggregate(min_cost=partsupp.PS_SUPPLYCOST.cast('double').min())
    innerq = q.view()

    q = part.join(partsupp, part.P_PARTKEY == partsupp.PS_PARTKEY)
    q = q.join(supplier, supplier.S_SUPPKEY == partsupp.PS_SUPPKEY)
    q = q.join(nation, supplier.S_NATIONKEY == nation.N_NATIONKEY)
    q = q.join(region, nation.N_REGIONKEY == region.R_REGIONKEY)
    q = q.materialize()
#    q = q.mutate(supplyfloat = q.PS_SUPPLYCOST.cast('double'))
    q = q.filter([
            part.P_PARTKEY == partsupp.PS_PARTKEY,
            q.P_SIZE == SIZE,
            q.P_TYPE.like('%'+TYPE),
            q.R_NAME == REGION,
#            q.supplyfloat == innerq.min_cost
#            q.supplyfloat.isin(innerq.min_cost)
            q.PS_SUPPLYCOST.cast('double') == innerq.min_cost
    ])

    return q
