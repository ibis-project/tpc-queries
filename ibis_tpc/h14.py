from .utils import add_date


def tpc_h14(con, DATE='1995-09-01'):
    '''Promotion Effect Query (Q14)

       This query monitors the market response to a promotion such as TV
       advertisements or a special campaign.'''

    lineitem = con.table('lineitem')
    part = con.table('part')
    q = lineitem
    q = q.join(part, lineitem.L_PARTKEY == part.P_PARTKEY)
    q = q.materialize()
    q = q.filter([
        q.L_SHIPDATE >= DATE,
        q.L_SHIPDATE < add_date(DATE, dm=1)
    ])

    revenue = q.L_EXTENDEDPRICE*(1-q.L_DISCOUNT)
    promo_revenue = q.P_TYPE.like('PROMO%').ifelse(revenue, 0)

    q = q.aggregate(promo_revenue=100*promo_revenue.sum()/revenue.sum())
    return q
