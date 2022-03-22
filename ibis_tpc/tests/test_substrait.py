import importlib
from datetime import date

import ibis_tpc
import pytest


modules = list(map(lambda x: f"ibis_tpc.h{x:02d}", range(1, 23)))

for module in modules:
    importlib.import_module(module)

test_params = [
    pytest.param(
        ibis_tpc.h01._tpc_h01,
        ("lineitem",),
        {"DATE": date(1998, 12, 1)},
        marks=[pytest.mark.duckdb_internal_error],
    ),
    pytest.param(
        ibis_tpc.h02._tpc_h02,
        ("part", "supplier", "partsupp", "nation", "region"),
        {},
        marks=[pytest.mark.corr_sub_query],
    ),
    pytest.param(
        ibis_tpc.h03._tpc_h03,
        ("customer", "orders", "lineitem"),
        {"DATE": date(1995, 3, 15)},
        marks=[pytest.mark.duckdb_internal_error],
    ),
    pytest.param(
        ibis_tpc.h04._tpc_h04,
        ("orders", "lineitem"),
        {"DATE": date(1993, 7, 1)},
        marks=[pytest.mark.duckdb_internal_error],
    ),
    pytest.param(
        ibis_tpc.h05._tpc_h05,
        ("customer", "orders", "lineitem", "supplier", "nation", "region"),
        {"DATE": date(1994, 1, 1)},
        marks=[pytest.mark.duckdb_internal_error],
    ),
    pytest.param(
        ibis_tpc.h06._tpc_h06,
        ("lineitem",),
        {"DATE": date(1994, 1, 1)},
        marks=[pytest.mark.duckdb_internal_error],
    ),
    pytest.param(
        ibis_tpc.h07._tpc_h07,
        ("supplier", "lineitem", "orders", "customer", "nation"),
        {"DATE": date(1995, 1, 1)},
        marks=[pytest.mark.corr_sub_query],
    ),
    pytest.param(
        ibis_tpc.h08._tpc_h08,
        ("part", "supplier", "lineitem", "orders", "customer", "region", "nation"),
        {"DATE": date(1995, 1, 1)},
        marks=[pytest.mark.corr_sub_query],
    ),
    pytest.param(
        ibis_tpc.h09._tpc_h09,
        ("part", "supplier", "lineitem", "partsupp", "orders", "nation"),
        {},
        marks=[pytest.mark.duckdb_internal_error],
    ),
    pytest.param(
        ibis_tpc.h10._tpc_h10,
        ("customer", "orders", "lineitem", "nation"),
        {"DATE": date(1993, 10, 1)},
        marks=[pytest.mark.duckdb_internal_error],
    ),
    pytest.param(
        ibis_tpc.h11._tpc_h11,
        ("partsupp", "supplier", "nation"),
        {},
        marks=[pytest.mark.duckdb_catalog_error],
    ),
    pytest.param(
        ibis_tpc.h12._tpc_h12,
        ("orders", "lineitem"),
        {"DATE": date(1994, 1, 1)},
        marks=[pytest.mark.duckdb_internal_error],
    ),
    pytest.param(
        ibis_tpc.h13._tpc_h13,
        ("customer", "orders"),
        {},
        marks=[pytest.mark.duckdb_catalog_error],
    ),
    pytest.param(
        ibis_tpc.h14._tpc_h14,
        ("lineitem", "part"),
        {"DATE": date(1995, 9, 1)},
        marks=[pytest.mark.corr_sub_query],
    ),
    pytest.param(
        ibis_tpc.h15._tpc_h15,
        ("lineitem", "supplier"),
        {"DATE": date(1996, 1, 1)},
        marks=[pytest.mark.ibis_substrait_error],
    ),
    pytest.param(
        ibis_tpc.h16._tpc_h16,
        ("partsupp", "part", "supplier"),
        {},
        marks=[pytest.mark.duckdb_internal_error],
    ),
    pytest.param(
        ibis_tpc.h17._tpc_h17,
        ("lineitem", "part"),
        {},
        marks=[pytest.mark.corr_sub_query],
    ),
    pytest.param(
        ibis_tpc.h18._tpc_h18,
        ("customer", "orders", "lineitem"),
        {},
        marks=[pytest.mark.duckdb_internal_error],
    ),
    pytest.param(
        ibis_tpc.h19._tpc_h19,
        ("lineitem", "part"),
        {},
        marks=[pytest.mark.duckdb_internal_error],
    ),
    pytest.param(
        ibis_tpc.h20._tpc_h20,
        ("supplier", "nation", "partsupp", "part", "lineitem"),
        {"DATE": date(1994, 1, 1)},
        marks=[pytest.mark.duckdb_catalog_error],
    ),
    pytest.param(
        ibis_tpc.h21._tpc_h21,
        ("supplier", "lineitem", "orders", "nation"),
        {},
        marks=[pytest.mark.duckdb_catalog_error],
    ),
    pytest.param(
        ibis_tpc.h22._tpc_h22,
        ("customer", "orders"),
        {},
        marks=[pytest.mark.duckdb_internal_error],
    ),
]


@pytest.mark.parametrize("func, tables, dates", test_params)
def test_tpc(con, compiler, func, tables, dates, request):
    tables = list(map(request.getfixturevalue, tables))
    query = func(tables, **dates)
    proto = compiler.compile(query)
    con.from_substrait(proto.SerializeToString())
