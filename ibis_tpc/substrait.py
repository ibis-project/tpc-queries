# noqa: D100

import duckdb
import ibis
from ibis.backends.base import BaseBackend
from ibis.backends.duckdb.datatypes import parse_type
from ibis_substrait.compiler.core import SubstraitCompiler


class TPCHBackend(BaseBackend):  # noqa: D101
    def __init__(self, fname="", scale_factor=0.1):  # noqa: D107
        self.con = duckdb.connect(fname)
        self.con.install_extension("substrait")
        self.con.load_extension("substrait")

        if not fname:
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

    def table(self, table):  # noqa: D102
        return self.tables.get(table)

    def current_database(self):  # noqa: D102
        ...

    def list_databases(self):  # noqa: D102
        ...

    def list_tables(self):  # noqa: D102
        ...

    def version(self):  # noqa: D102
        return "awesome"


def unbound_from_duckdb(table):  # noqa: D103
    return ibis.table(
        list(zip(table.columns, map(parse_type, table.dtypes))), name=table.alias
    )
