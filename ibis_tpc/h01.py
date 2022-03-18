"Pricing Summary Report Query (Q1)"
from .utils import add_date


def tpc_h01(con, DELTA=-90, DATE="1998-12-01"):
    """
    The Pricing Summary Report Query provides a summary pricing report for all
    lineitems shipped as of a given date.  The  date is  within  60  - 120 days
    of  the  greatest  ship  date  contained  in  the database.  The query
    lists totals  for extended  price,  discounted  extended price, discounted
    extended price  plus  tax,  average  quantity, average extended price,  and
    average discount.  These  aggregates  are grouped  by RETURNFLAG  and
    LINESTATUS, and  listed  in ascending  order of RETURNFLAG and  LINESTATUS.
    A  count  of the  number  of  lineitems in each  group  is included.
    """

    lineitem = con.table("lineitem")

    tables = (lineitem,)

    return _tpc_h01(tables, DELTA, DATE)


def _tpc_h01(tables, DELTA=-90, DATE="1998-12-01"):

    lineitem, = tables

    interval = add_date(DATE, dd=DELTA)
    q = lineitem.filter(lineitem.l_shipdate < interval)
    discount_price = lineitem.l_extendedprice * (1 - lineitem.l_discount)
    charge = discount_price * (1 + lineitem.l_tax)
    q = q.group_by(["l_returnflag", "l_linestatus"])
    q = q.order_by(["l_returnflag", "l_linestatus"])
    q = q.aggregate(
        sum_qty=lineitem.l_quantity.sum(),
        sum_base_price=lineitem.l_extendedprice.sum(),
        sum_disc_price=discount_price.sum(),
        sum_charge=charge.sum(),
        avg_qty=lineitem.l_quantity.mean(),
        avg_price=lineitem.l_extendedprice.mean(),
        avg_disc=lineitem.l_discount.mean(),
        count_order=lineitem.count(),
    )
    return q
