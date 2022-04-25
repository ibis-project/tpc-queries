from .utils import add_date


def tpc_h14(con, DATE="1995-09-01"):
    """Promotion Effect Query (Q14)

    This query monitors the market response to a promotion such as TV
    advertisements or a special campaign."""

    lineitem = con.table("lineitem")
    part = con.table("part")
    q = lineitem
    q = q.join(part, lineitem.l_partkey == part.p_partkey)
    q = q.filter([q.l_shipdate >= DATE, q.l_shipdate < add_date(DATE, dm=1)])

    revenue = q.l_extendedprice * (1 - q.l_discount)
    promo_revenue = q.p_type.like("PROMO%").ifelse(revenue, 0)

    q = q.aggregate(promo_revenue=100 * promo_revenue.sum() / revenue.sum())
    return q
