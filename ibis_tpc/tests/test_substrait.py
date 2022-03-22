import importlib
from contextlib import nullcontext
from datetime import date

import ibis_tpc
import pytest

modules = list(map(lambda x: f"ibis_tpc.h{x:02d}", range(1, 23)))

for module in modules:
    importlib.import_module(module)

serialize_deserialize = [
    pytest.param(
        ibis_tpc.h01.tpc_h01,
        {"DATE": date(1998, 12, 1)},
        pytest.raises(
            RuntimeError, match=r".*Only single grouping sets are supported.*"
        ),
    ),
    pytest.param(
        ibis_tpc.h02.tpc_h02,
        {},
        pytest.raises(TypeError, match=r".*must be instance of same class.*"),
    ),
    pytest.param(
        ibis_tpc.h03.tpc_h03,
        {"DATE": date(1995, 3, 15)},
        pytest.raises(
            RuntimeError, match=r".*Only single grouping sets are supported.*"
        ),
    ),
    pytest.param(
        ibis_tpc.h04.tpc_h04,
        {"DATE": date(1993, 7, 1)},
        pytest.raises(RuntimeError, match=r".*Error: 16.*"),
    ),
    pytest.param(
        ibis_tpc.h05.tpc_h05,
        {"DATE": date(1994, 1, 1)},
        pytest.raises(RuntimeError, match=r".*Error: 2.*"),
    ),
    pytest.param(
        ibis_tpc.h06.tpc_h06,
        {"DATE": date(1994, 1, 1)},
        pytest.raises(RuntimeError, match=r".*Error: 16.*"),
    ),
    pytest.param(
        ibis_tpc.h07.tpc_h07,
        {"DATE": date(1995, 1, 1)},
        pytest.raises(NotImplementedError, match=r".*SelfReference.*"),
    ),
    pytest.param(
        ibis_tpc.h08.tpc_h08,
        {"DATE": date(1995, 1, 1)},
        pytest.raises(NotImplementedError, match=r".*SelfReference.*"),
    ),
    pytest.param(
        ibis_tpc.h09.tpc_h09,
        {},
        pytest.raises(
            RuntimeError, match=r".*Only single grouping sets are supported.*"
        ),
    ),
    pytest.param(
        ibis_tpc.h10.tpc_h10,
        {"DATE": date(1993, 10, 1)},
        pytest.raises(
            RuntimeError, match=r".*Only single grouping sets are supported.*"
        ),
    ),
    pytest.param(
        ibis_tpc.h11.tpc_h11,
        {},
        pytest.raises(RuntimeError, match=r".*Catalog Error.*"),
    ),
    pytest.param(
        ibis_tpc.h12.tpc_h12,
        {"DATE": date(1994, 1, 1)},
        pytest.raises(RuntimeError, match=r".*Error: 2.*"),
    ),
    pytest.param(
        ibis_tpc.h13.tpc_h13,
        {},
        pytest.raises(RuntimeError, match=r".*Catalog Error.*"),
    ),
    pytest.param(
        ibis_tpc.h14.tpc_h14,
        {"DATE": date(1995, 9, 1)},
        pytest.raises(TypeError, match=r".*must be instance of same class.*"),
    ),
    pytest.param(
        ibis_tpc.h15.tpc_h15,
        {"DATE": date(1996, 1, 1)},
        pytest.raises(AssertionError, match=r".*non-empty child_rel_field_offsets.*"),
    ),
    pytest.param(
        ibis_tpc.h16.tpc_h16,
        {},
        pytest.raises(
            RuntimeError, match=r".*Only single grouping sets are supported.*"
        ),
    ),
    pytest.param(
        ibis_tpc.h17.tpc_h17,
        {},
        pytest.raises(TypeError, match=r".*must be instance of same class.*"),
    ),
    pytest.param(
        ibis_tpc.h18.tpc_h18,
        {},
        pytest.raises(
            RuntimeError, match=r".*Only single grouping sets are supported.*"
        ),
    ),
    pytest.param(
        ibis_tpc.h19.tpc_h19,
        {},
        pytest.raises(RuntimeError, match=r".*Error: 2.*"),
    ),
    pytest.param(
        ibis_tpc.h20.tpc_h20,
        {"DATE": date(1994, 1, 1)},
        pytest.raises(RuntimeError, match=r".*Catalog Error.*"),
    ),
    pytest.param(
        ibis_tpc.h21.tpc_h21,
        {},
        pytest.raises(RuntimeError, match=r".*Catalog Error.*"),
    ),
    pytest.param(
        ibis_tpc.h22.tpc_h22,
        {},
        pytest.raises(RuntimeError, match=r".*Error: 2.*"),
    ),
]


@pytest.mark.parametrize("tpc_func, kwargs, expectation", serialize_deserialize)
def test_send_to_duckdb(con, compiler, tpc_func, expectation, kwargs):
    query = tpc_func(con, **kwargs)

    # The con.con here points to the underlying DuckDB connection
    with expectation:
        proto = compiler.compile(query)
        con.con.from_substrait(proto.SerializeToString())


serialize = [
    pytest.param(
        ibis_tpc.h01.tpc_h01,
        {"DATE": date(1998, 12, 1)},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h02.tpc_h02,
        {},
        pytest.raises(TypeError, match=r".*must be instance of same class.*"),
    ),
    pytest.param(
        ibis_tpc.h03.tpc_h03,
        {"DATE": date(1995, 3, 15)},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h04.tpc_h04,
        {"DATE": date(1993, 7, 1)},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h05.tpc_h05,
        {"DATE": date(1994, 1, 1)},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h06.tpc_h06,
        {"DATE": date(1994, 1, 1)},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h07.tpc_h07,
        {"DATE": date(1995, 1, 1)},
        pytest.raises(NotImplementedError, match=r".*SelfReference.*"),
    ),
    pytest.param(
        ibis_tpc.h08.tpc_h08,
        {"DATE": date(1995, 1, 1)},
        pytest.raises(NotImplementedError, match=r".*SelfReference.*"),
    ),
    pytest.param(
        ibis_tpc.h09.tpc_h09,
        {},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h10.tpc_h10,
        {"DATE": date(1993, 10, 1)},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h11.tpc_h11,
        {},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h12.tpc_h12,
        {"DATE": date(1994, 1, 1)},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h13.tpc_h13,
        {},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h14.tpc_h14,
        {"DATE": date(1995, 9, 1)},
        pytest.raises(TypeError, match=r".*must be instance of same class.*"),
    ),
    pytest.param(
        ibis_tpc.h15.tpc_h15,
        {"DATE": date(1996, 1, 1)},
        pytest.raises(AssertionError, match=r".*non-empty child_rel_field_offsets.*"),
    ),
    pytest.param(
        ibis_tpc.h16.tpc_h16,
        {},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h17.tpc_h17,
        {},
        pytest.raises(TypeError, match=r".*must be instance of same class.*"),
    ),
    pytest.param(
        ibis_tpc.h18.tpc_h18,
        {},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h19.tpc_h19,
        {},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h20.tpc_h20,
        {"DATE": date(1994, 1, 1)},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h21.tpc_h21,
        {},
        nullcontext(),
    ),
    pytest.param(
        ibis_tpc.h22.tpc_h22,
        {},
        nullcontext(),
    ),
]


@pytest.mark.parametrize("tpc_func, kwargs, expectation", serialize)
def test_compile(con, compiler, tpc_func, kwargs, expectation):
    query = tpc_func(con, **kwargs)
    with expectation:
        compiler.compile(query)
