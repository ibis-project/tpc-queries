import ibis


ibis.options.sql.default_limit = 100000


def tpc_h16(
    con, BRAND="Brand#45", TYPE="MEDIUM POLISHED", SIZES=(49, 14, 23, 45, 19, 3, 36, 9)
):
    """Parts/Supplier Relationship Query (Q16)

    This query finds out how many suppliers can supply parts with given
    attributes. It might be used, for example, to determine whether there is
    a sufficient number of suppliers for heavily ordered parts."""

    partsupp = con.table("partsupp")
    part = con.table("part")
    supplier = con.table("supplier")

    q = partsupp.join(part, part.p_partkey == partsupp.ps_partkey)
    q = q.filter(
        [
            q.p_brand != BRAND,
            ~q.p_type.like(f"{TYPE}%"),
            q.p_size.isin(SIZES),
            ~q.ps_suppkey.isin(
                supplier.filter(
                    [supplier.s_comment.like("%Customer%Complaints%")]
                ).s_suppkey
            ),
        ]
    )
    gq = q.group_by([q.p_brand, q.p_type, q.p_size])
    q = gq.aggregate(supplier_cnt=q.ps_suppkey.nunique())
    q = q.order_by([ibis.desc(q.supplier_cnt), q.p_brand, q.p_type, q.p_size])
    return q
