'Minimum Cost Supplier Query (Q2)'


import ibis


def tpc_h02(con, REGION='EUROPE', SIZE=15, TYPE='BRASS'):
    part = con.table("part")
    supplier = con.table("supplier")
    partsupp = con.table("partsupp")
    nation = con.table("nation")
    region = con.table("region")

    expr = (
        part.join(partsupp, part.P_PARTKEY == partsupp.PS_PARTKEY)
        .join(supplier, supplier.S_SUPPKEY == partsupp.PS_SUPPKEY)
        .join(nation, supplier.S_NATIONKEY == nation.N_NATIONKEY)
        .join(region, nation.N_REGIONKEY == region.R_REGIONKEY)
    ).materialize()

    subexpr = (
        partsupp.join(supplier, supplier.S_SUPPKEY == partsupp.PS_SUPPKEY)
        .join(nation, supplier.S_NATIONKEY == nation.N_NATIONKEY)
        .join(region, nation.N_REGIONKEY == region.R_REGIONKEY)
    )

    conditional_min = subexpr[region.R_NAME == REGION].PS_SUPPLYCOST.min()

    filters = [
        expr.P_SIZE == SIZE,
        expr.P_TYPE.like("%"+TYPE),
        expr.R_NAME == REGION,
        expr.PS_SUPPLYCOST == conditional_min,
    ]
    q = expr.filter(filters)
    q = q.select([
        q.S_ACCTBAL,
        q.S_NAME,
        q.N_NAME,
        q.P_PARTKEY,
        q.P_MFGR,
        q.S_ADDRESS,
        q.S_PHONE,
        q.S_COMMENT,
        q.R_NAME,
        q.P_TYPE,
    ])

    return q.sort_by(
                [
                    ibis.desc(q.S_ACCTBAL),
                    q.N_NAME,
                    q.S_NAME,
                    q.P_PARTKEY,
                ]
            ).limit(100)
