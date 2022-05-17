import importlib
from datetime import date

import ibis_tpc
import pytest
from ibis_substrait.compiler.decompile import decompile

modules = list(map(lambda x: f"ibis_tpc.h{x:02d}", range(1, 23)))

for module in modules:
    importlib.import_module(module)

serialize_deserialize = [
    pytest.param(
        ibis_tpc.h01.tpc_h01,
        {"DATE": date(1998, 12, 1)},
    ),
    pytest.param(
        ibis_tpc.h02.tpc_h02,
        {},
        marks=pytest.mark.xfail(reason="correlated_subquery"),
    ),
    pytest.param(
        ibis_tpc.h03.tpc_h03,
        {"DATE": date(1995, 3, 15)},
    ),
    pytest.param(
        ibis_tpc.h04.tpc_h04,
        {"DATE": date(1993, 7, 1)},
        marks=pytest.mark.xfail(reason="scalar function 'any'"),
    ),
    pytest.param(
        ibis_tpc.h05.tpc_h05,
        {"DATE": date(1994, 1, 1)},
    ),
    pytest.param(
        ibis_tpc.h06.tpc_h06,
        {"DATE": date(1994, 1, 1)},
    ),
    pytest.param(
        ibis_tpc.h07.tpc_h07,
        {"DATE": date(1995, 1, 1)},
        marks=pytest.mark.xfail(reason="correlated_subquery"),
    ),
    pytest.param(
        ibis_tpc.h08.tpc_h08,
        {"DATE": date(1995, 1, 1)},
        marks=pytest.mark.xfail(reason="correlated_subquery"),
    ),
    pytest.param(
        ibis_tpc.h09.tpc_h09,
        {},
        marks=pytest.mark.xfail(reason="scalar function 'cast'"),
    ),
    pytest.param(
        ibis_tpc.h10.tpc_h10,
        {"DATE": date(1993, 10, 1)},
    ),
    pytest.param(
        ibis_tpc.h11.tpc_h11,
        {},
    ),
    pytest.param(
        ibis_tpc.h12.tpc_h12,
        {"DATE": date(1994, 1, 1)},
        marks=pytest.mark.xfail(reason="scalar function 'values'"),
    ),
    pytest.param(
        ibis_tpc.h13.tpc_h13,
        {},
        marks=pytest.mark.xfail(reason="how to translate not?"),
    ),
    pytest.param(
        ibis_tpc.h14.tpc_h14,
        {"DATE": date(1995, 9, 1)},
        marks=pytest.mark.xfail(reason="how to translate ops.SearchedCase?"),
    ),
    pytest.param(
        ibis_tpc.h15.tpc_h15,
        {"DATE": date(1996, 1, 1)},
        marks=pytest.mark.xfail(reason="correlated_subquery"),
    ),
    pytest.param(
        ibis_tpc.h16.tpc_h16,
        {},
        marks=pytest.mark.xfail(reason="how to translate not?"),
    ),
    pytest.param(
        ibis_tpc.h17.tpc_h17,
        {},
        marks=pytest.mark.xfail(reason="correlated_subquery"),
    ),
    pytest.param(
        ibis_tpc.h18.tpc_h18,
        {},
        marks=pytest.mark.xfail(reason="duckdb INTERNAL ERROR (no cast)"),
    ),
    pytest.param(
        ibis_tpc.h19.tpc_h19,
        {},
        # note that this works elsewhere, probably because whatever "ValueList"
        # is being used for gets optimized out?
        marks=pytest.mark.xfail(reason="scalar function 'values'"),
    ),
    pytest.param(
        ibis_tpc.h20.tpc_h20,
        {"DATE": date(1994, 1, 1)},
        marks=pytest.mark.xfail(reason="duckdb INTERNAL ERROR (no cast)"),
    ),
    pytest.param(
        ibis_tpc.h21.tpc_h21,
        {},
        marks=pytest.mark.xfail(reason="scalar function 'any'"),
    ),
    pytest.param(
        ibis_tpc.h22.tpc_h22,
        {},
        marks=pytest.mark.xfail(reason="scalar function 'values'"),
    ),
]


@pytest.mark.parametrize("tpc_func, kwargs", serialize_deserialize)
def test_send_to_duckdb(con, compiler, tpc_func, kwargs):
    query = tpc_func(con, **kwargs)

    # The con.con here points to the underlying DuckDB connection
    proto = compiler.compile(query)
    con.con.from_substrait(proto.SerializeToString())


serialize = [
    pytest.param(
        ibis_tpc.h01.tpc_h01,
        {"DATE": date(1998, 12, 1)},
    ),
    pytest.param(
        ibis_tpc.h02.tpc_h02,
        {},
        marks=pytest.mark.xfail,
    ),
    pytest.param(
        ibis_tpc.h03.tpc_h03,
        {"DATE": date(1995, 3, 15)},
    ),
    pytest.param(
        ibis_tpc.h04.tpc_h04,
        {"DATE": date(1993, 7, 1)},
    ),
    pytest.param(
        ibis_tpc.h05.tpc_h05,
        {"DATE": date(1994, 1, 1)},
    ),
    pytest.param(
        ibis_tpc.h06.tpc_h06,
        {"DATE": date(1994, 1, 1)},
    ),
    pytest.param(
        ibis_tpc.h07.tpc_h07,
        {"DATE": date(1995, 1, 1)},
        marks=pytest.mark.xfail,
    ),
    pytest.param(
        ibis_tpc.h08.tpc_h08,
        {"DATE": date(1995, 1, 1)},
        marks=pytest.mark.xfail,
    ),
    pytest.param(
        ibis_tpc.h09.tpc_h09,
        {},
    ),
    pytest.param(
        ibis_tpc.h10.tpc_h10,
        {"DATE": date(1993, 10, 1)},
    ),
    pytest.param(
        ibis_tpc.h11.tpc_h11,
        {},
    ),
    pytest.param(
        ibis_tpc.h12.tpc_h12,
        {"DATE": date(1994, 1, 1)},
    ),
    pytest.param(
        ibis_tpc.h13.tpc_h13,
        {},
    ),
    pytest.param(
        ibis_tpc.h14.tpc_h14,
        {"DATE": date(1995, 9, 1)},
        marks=pytest.mark.xfail,
    ),
    pytest.param(
        ibis_tpc.h15.tpc_h15,
        {"DATE": date(1996, 1, 1)},
        marks=pytest.mark.xfail,
    ),
    pytest.param(
        ibis_tpc.h16.tpc_h16,
        {},
    ),
    pytest.param(
        ibis_tpc.h17.tpc_h17,
        {},
        marks=pytest.mark.xfail,
    ),
    pytest.param(
        ibis_tpc.h18.tpc_h18,
        {},
    ),
    pytest.param(
        ibis_tpc.h19.tpc_h19,
        {},
    ),
    pytest.param(
        ibis_tpc.h20.tpc_h20,
        {"DATE": date(1994, 1, 1)},
    ),
    pytest.param(
        ibis_tpc.h21.tpc_h21,
        {},
    ),
    pytest.param(
        ibis_tpc.h22.tpc_h22,
        {},
    ),
]


@pytest.mark.parametrize("tpc_func, kwargs", serialize)
def test_compile(con, compiler, tpc_func, kwargs):
    query = tpc_func(con, **kwargs)
    compiler.compile(query)


roundtrip = [
    pytest.param(
        ibis_tpc.h01.tpc_h01,
        {"DATE": date(1998, 12, 1)},
    ),
    pytest.param(
        ibis_tpc.h02.tpc_h02,
        {},
        marks=pytest.mark.xfail,
    ),
    pytest.param(
        ibis_tpc.h03.tpc_h03,
        {"DATE": date(1995, 3, 15)},
    ),
    pytest.param(
        ibis_tpc.h04.tpc_h04,
        {"DATE": date(1993, 7, 1)},
        marks=pytest.mark.xfail(reason="column index out of bounds"),
    ),
    pytest.param(
        ibis_tpc.h05.tpc_h05,
        {"DATE": date(1994, 1, 1)},
    ),
    pytest.param(
        ibis_tpc.h06.tpc_h06,
        {"DATE": date(1994, 1, 1)},
    ),
    pytest.param(
        ibis_tpc.h07.tpc_h07,
        {"DATE": date(1995, 1, 1)},
        marks=pytest.mark.xfail,
    ),
    pytest.param(
        ibis_tpc.h08.tpc_h08,
        {"DATE": date(1995, 1, 1)},
        marks=pytest.mark.xfail,
    ),
    pytest.param(
        ibis_tpc.h09.tpc_h09,
        {},
        marks=pytest.mark.xfail(reason="pop from an empty deque on decompile"),
    ),
    pytest.param(
        ibis_tpc.h10.tpc_h10,
        {"DATE": date(1993, 10, 1)},
    ),
    pytest.param(
        ibis_tpc.h11.tpc_h11,
        {},
        marks=pytest.mark.xfail(reason="results don't match"),
    ),
    pytest.param(
        ibis_tpc.h12.tpc_h12,
        {"DATE": date(1994, 1, 1)},
    ),
    pytest.param(
        ibis_tpc.h13.tpc_h13,
        {},
        marks=pytest.mark.xfail(reason="wrong type for result of `count()`"),
    ),
    pytest.param(
        ibis_tpc.h14.tpc_h14,
        {"DATE": date(1995, 9, 1)},
        marks=pytest.mark.xfail,
    ),
    pytest.param(
        ibis_tpc.h15.tpc_h15,
        {"DATE": date(1996, 1, 1)},
        marks=pytest.mark.xfail,
    ),
    pytest.param(
        ibis_tpc.h16.tpc_h16,
        {},
    ),
    pytest.param(
        ibis_tpc.h17.tpc_h17,
        {},
        marks=pytest.mark.xfail,
    ),
    pytest.param(
        ibis_tpc.h18.tpc_h18,
        {},
    ),
    pytest.param(
        ibis_tpc.h19.tpc_h19,
        {},
    ),
    pytest.param(
        ibis_tpc.h20.tpc_h20,
        {"DATE": date(1994, 1, 1)},
    ),
    pytest.param(
        ibis_tpc.h21.tpc_h21,
        {},
        marks=pytest.mark.xfail(reason="pop from an empty deque on decompile"),
    ),
    pytest.param(
        ibis_tpc.h22.tpc_h22,
        {},
        marks=pytest.mark.xfail(
            reason="bad type comparison which results from a bad field lookup"
        ),
    ),
]


@pytest.mark.parametrize("tpc_func, kwargs", roundtrip)
def test_roundtrip(con, duckcon, compiler, tpc_func, kwargs):
    query = tpc_func(con, **kwargs)
    try:
        proto = compiler.compile(query)
    except Exception:
        pytest.skip("compilation failed, not attempting to decompile")
    (result,) = decompile(proto)

    query_res = duckcon.execute(query)
    result_res = duckcon.execute(result)
    # duckdb is stupidly fast and this lets us compare the actual results
    # and ignores any differences due to table ordering in the two expressions
    # TODO: fix table ordering?  do we care?
    assert query_res.equals(result_res)
