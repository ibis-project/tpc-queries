# noqa: D100

import duckdb
import ibis
from ibis.backends.base import BaseBackend


class TPCHBackend(BaseBackend):  # noqa: D101
    def __init__(self, fname="", scale_factor=0.1):  # noqa: D107
        if fname:
            con = ibis.duckdb.connect(fname)
        else:
            con = ibis.duckdb.connect()

        if not fname:
            con.raw_sql(f"CALL dbgen(sf={scale_factor})")

        self.tables = {
            name: con.tables.get(name).unbind() for name in con.list_tables()
        }
        del con

        self.con = duckdb.connect(fname)
        self.con.install_extension("substrait")
        self.con.load_extension("substrait")
        self.con.execute(f"CALL dbgen(sf={scale_factor})")

    def table(self, table):  # noqa: D102
        return self.tables.get(table)

    def create_table(self, *args, **kwargs):  # noqa: D102
        ...

    def create_view(self, *args, **kwargs):  # noqa: D102
        ...

    def current_database(self):  # noqa: D102
        ...

    def list_databases(self):  # noqa: D102
        ...

    def list_tables(self):  # noqa: D102
        ...

    def version(self):  # noqa: D102
        return "awesome"
