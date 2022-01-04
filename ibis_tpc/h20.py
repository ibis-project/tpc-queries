from .utils import add_date


def tpc_h20(con, COLOR='forest', DATE='1994-01-01', NATION='CANADA'):
    '''Potential Part Promotion Query (Q20)

    The Potential Part Promotion Query identifies suppliers in a particular
    nation having selected parts that may be candidates for a promotional
    offer.'''

    supplier = con.table('supplier')
    nation = con.table('nation')
    partsupp = con.table('partsupp')
    part = con.table('part')
    lineitem = con.table('lineitem')

    q1 = supplier.join(nation, supplier.S_NATIONKEY == nation.N_NATIONKEY)
    q1 = q1.materialize()

    q3 = part.filter([part.P_NAME.like(f'{COLOR}%')])
    q2 = partsupp

    q4 = lineitem.filter([
        lineitem.L_PARTKEY == q2.PS_PARTKEY,
        lineitem.L_SUPPKEY == q2.PS_SUPPKEY,
        lineitem.L_SHIPDATE >= DATE,
        lineitem.L_SHIPDATE < add_date(DATE, dy=1),
    ])

    q2 = q2.filter([
        partsupp.PS_PARTKEY.isin(q3.P_PARTKEY),
        partsupp.PS_AVAILQTY > .5*q4.L_QUANTITY.sum()
    ])

    q1 = q1.filter([
        q1.N_NAME == NATION,
        q1.S_SUPPKEY.isin(q2.PS_SUPPKEY)
    ])

    q1 = q1[q1.S_NAME, q1.S_ADDRESS]

    return q1.sort_by(q1.S_NAME)
