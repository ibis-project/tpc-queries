# noqa: D100

import duckdb
import ibis
from ibis.backends.base import BaseBackend


class TPCHBackend(BaseBackend):  # noqa: D101
    def __init__(self, fname="", scale_factor=0.2):  # noqa: D107
        if fname:
            con = ibis.duckdb.connect(fname)
        else:
            con = ibis.duckdb.connect()

        if not fname:
            con.raw_sql("CALL dbgen(sf=0)")

        self.tables = {
            name: con.tables.get(name).unbind() for name in con.list_tables()
        }
        del con

        self.con = duckdb.connect(fname)
        self.con.install_extension("substrait")
        self.con.load_extension("substrait")
        if not fname:
            self.con.execute(f"CALL dbgen(sf={scale_factor})")
            # sf ~< 0.17 won't necessarily generate any results for certain queries

    def table(self, table):  # noqa: D102
        return self.tables.get(table)

    def create_table(self, *args, **kwargs):  # noqa: D102
        ...

    def drop_table(self, *args, **kwargs):  # noqa: D102
        ...

    def create_view(self, *args, **kwargs):  # noqa: D102
        ...

    def drop_view(self, *args, **kwargs):  # noqa: D102
        ...

    def current_database(self):  # noqa: D102
        ...

    def list_databases(self):  # noqa: D102
        ...

    def list_tables(self):  # noqa: D102
        ...

    def version(self):  # noqa: D102
        return "awesome"
