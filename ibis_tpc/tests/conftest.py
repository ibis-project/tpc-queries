import duckdb
import ibis
import pytest
from ibis.backends.base import BaseBackend
from ibis.backends.duckdb.parser import parse_type
from ibis_substrait.compiler.core import SubstraitCompiler


def unbound_from_duckdb(table):
    return ibis.table(
        list(zip(table.columns, map(parse_type, table.dtypes))), name=table.alias
    )


class TPCHBackend(BaseBackend):
    def __init__(self, fname=":memory:", scale_factor=0.1):
        self.con = duckdb.connect(fname)

        self.con.execute(f"CALL dbgen(sf={scale_factor})")

        _tables = self.con.execute("PRAGMA show_tables").fetchall()
        _tables = map(lambda x: x[0], _tables)

        self.tables = {
            table.alias: unbound_from_duckdb(table)
            for table in map(
                self.con.table,
                _tables,
            )
        }

    def table(self, table):
        return self.tables.get(table)

    def current_database(self):
        ...

    def list_databases(self):
        ...

    def list_tables(self):
        ...

    def version(self):
        return "awesome"


@pytest.fixture(scope="module")
def con():
    yield TPCHBackend()


@pytest.fixture(scope="module")
def compiler():
    yield SubstraitCompiler()
