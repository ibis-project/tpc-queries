from datetime import date, timedelta


def query_tpch_q01(con, DELTA=90):
    '''
    The Pricing Summary Report Query provides a summary pricing report for all
    lineitems shipped as of a given date.  The  date is  within  60  - 120 days
    of  the  greatest  ship  date  contained  in  the database.  The query
    lists totals  for extended  price,  discounted  extended price, discounted
    extended price  plus  tax,  average  quantity, average extended price,  and
    average discount.  These  aggregates  are grouped  by RETURNFLAG  and
    LINESTATUS, and  listed  in ascending  order of RETURNFLAG and  LINESTATUS.
    A  count  of the  number  of  lineitems in each  group  is included.
    '''

    t = con.table('lineitem')

    interval = date.fromisoformat('1998-12-01') - timedelta(days=DELTA)
    q = t.filter(t.L_SHIPDATE < interval)
    discount_price = t.L_EXTENDEDPRICE*(1-t.L_DISCOUNT)
    charge = discount_price*(1+t.L_TAX)
    q = q.group_by(['L_RETURNFLAG', 'L_LINESTATUS'])
    q = q.order_by(['L_RETURNFLAG', 'L_LINESTATUS'])
    q = q.aggregate(sum_qty=t.L_QUANTITY.sum(),
                    sum_base_price=t.L_EXTENDEDPRICE.sum(),
                    sum_disc_price=discount_price.sum(),
                    sum_charge=charge.sum(),
                    avg_qty=t.L_QUANTITY.mean(),
                    avg_price=t.L_EXTENDEDPRICE.mean(),
                    avg_disc=t.L_DISCOUNT.mean(),
                    count_order=t.count(),
                    )
    return q
