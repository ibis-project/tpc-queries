import duckdb
import ibis
import pytest
from ibis.backends.duckdb.parser import parse_type
from ibis_substrait.compiler.core import SubstraitCompiler


def unbound_from_duckdb(table):
    return ibis.table(
        list(zip(table.columns, map(parse_type, table.dtypes))), name=table.alias
    )


@pytest.fixture(scope="module")
def con():
    yield duckdb.connect("tpch.ddb")


@pytest.fixture(scope="module")
def compiler():
    yield SubstraitCompiler()


@pytest.fixture(scope="module")
def part(con):
    table = con.table("part")
    yield unbound_from_duckdb(table)


@pytest.fixture(scope="module")
def supplier(con):
    table = con.table("supplier")
    yield unbound_from_duckdb(table)


@pytest.fixture(scope="module")
def partsupp(con):
    table = con.table("partsupp")
    yield unbound_from_duckdb(table)


@pytest.fixture(scope="module")
def nation(con):
    table = con.table("nation")
    yield unbound_from_duckdb(table)


@pytest.fixture(scope="module")
def region(con):
    table = con.table("region")
    yield unbound_from_duckdb(table)


@pytest.fixture(scope="module")
def lineitem(con):
    table = con.table("lineitem")
    yield unbound_from_duckdb(table)


@pytest.fixture(scope="module")
def customer(con):
    table = con.table("customer")
    yield unbound_from_duckdb(table)


@pytest.fixture(scope="module")
def orders(con):
    table = con.table("orders")
    yield unbound_from_duckdb(table)


def pytest_runtest_call(item):
    """Dynamically add various custom markers."""
    #nodeid = item.nodeid

    for marker in item.iter_markers(name="duckdb_internal_error"):
        item.add_marker(
            pytest.mark.xfail(reason="DuckDB Internal Error")
            )
    for marker in item.iter_markers(name="corr_sub_query"):
        item.add_marker(
            pytest.mark.xfail(reason="Substrait doesn't support correlated subqueries")
            )
    for marker in item.iter_markers(name="duckdb_catalog_error"):
        item.add_marker(
            pytest.mark.xfail(reason="DuckDB Catalog Error")
            )
    for marker in item.iter_markers(name="ibis_substrait_error"):
        item.add_marker(
            pytest.mark.xfail(reason="Ibis substrait compilation failure")
            )
