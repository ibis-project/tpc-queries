import ibis


def tpc_h02(con, REGION="EUROPE", SIZE=25, TYPE="BRASS"):
    "Minimum Cost Supplier Query (Q2)"

    part = con.table("part")
    supplier = con.table("supplier")
    partsupp = con.table("partsupp")
    nation = con.table("nation")
    region = con.table("region")

    expr = (
        part.join(partsupp, part.p_partkey == partsupp.ps_partkey)
        .join(supplier, supplier.s_suppkey == partsupp.ps_suppkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .join(region, nation.n_regionkey == region.r_regionkey)
    )

    subexpr = (
        partsupp.join(supplier, supplier.s_suppkey == partsupp.ps_suppkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .join(region, nation.n_regionkey == region.r_regionkey)
    )

    subexpr = subexpr[
        (subexpr.r_name == REGION) & (expr.p_partkey == subexpr.ps_partkey)
    ]

    filters = [
        expr.p_size == SIZE,
        expr.p_type.like("%" + TYPE),
        expr.r_name == REGION,
        expr.ps_supplycost == subexpr.ps_supplycost.min(),
    ]
    q = expr.filter(filters)

    q = q.select(
        [
            q.s_acctbal,
            q.s_name,
            q.n_name,
            q.p_partkey,
            q.p_mfgr,
            q.s_address,
            q.s_phone,
            q.s_comment,
        ]
    )

    return q.order_by(
        [
            ibis.desc(q.s_acctbal),
            q.n_name,
            q.s_name,
            q.p_partkey,
        ]
    ).limit(100)
