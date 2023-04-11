#!/usr/bin/env python3

import datetime
import glob
import itertools
import json
import math
import os
import time
import warnings
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List

import click
import ibis
import pandas
from multipledispatch import dispatch

from .substrait import TPCHBackend

g_debug = False

# Currently very noisy warnings coming from SQLAlchemy via Ibis
warnings.filterwarnings("ignore", module="ibis")


def fmt(v):
    if isinstance(v, float):
        return "%.03f" % v
    else:
        return str(v)


def out_txt(s, outdir, fn):
    if outdir:
        with open(Path(outdir) / fn, mode="w") as f:
            print(s, file=f, flush=True)


def out_jsonl(rows: List[Dict[str, Any]], outdir, fn):
    class DateEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, pandas.Timestamp):
                return str(obj)
            elif isinstance(obj, Decimal):
                return float(obj)
            elif isinstance(obj, datetime.date):
                return str(obj)
            return json.JSONEncoder.default(self, obj)

    if outdir:
        with open(Path(outdir) / fn, mode="w") as fp:
            for r in rows:
                print(json.dumps(r, cls=DateEncoder), file=fp, flush=True)


class Runner:
    def __init__(self, interface="sqlite", backend="sqlite"):
        self.interface = interface
        self.backend = backend
        self.prints = []
        self.warns = []
        self.errors = []

    def setup(self, db="tpch.db"):
        self.prints = []
        self.warns = []
        self.errors = []

    def teardown(self):
        pass

    def run(self, qid, outdir=None, backend="sqlite"):
        pass

    def print(self, s):
        self.prints.append(s.strip())

    def warn(self, s):
        self.warns.append(s.strip())

    def error(self, s):
        self.errors.append(s.strip())

    def info(self):
        return dict(interface=self.interface, backend=self.backend)


class SqliteRunner(Runner):
    def setup(self, db="tpch.db"):
        super().setup(db=db)
        import sqlite3

        self.con = sqlite3.connect(db)
        self.con.row_factory = sqlite3.Row

    def run(self, qid, outdir=None, backend="sqlite"):
        cur = self.con.cursor()

        sql = Path(f"sqlite_tpc/{qid}.sql").read_text()
        t1 = time.time()
        cur.execute(sql)
        rows = cur.fetchall()
        t2 = time.time()
        rows = list(dict(r) for r in rows)
        return rows, t2 - t1

    def info(self):
        import sqlite3

        return dict(
            interface=self.interface,
            backend=f"{self.backend}",
            sqlite_version=sqlite3.sqlite_version,
        )


class DuckDBRunner(Runner):
    def setup(self, db="tpch.ddb"):
        super().setup(db=db)
        import duckdb

        self.con = duckdb.connect(db)

    def run(self, qid, outdir=None, backend="duckdb"):
        cur = self.con.cursor()

        sql = Path(f"sqlite_tpc/{qid}.sql").read_text()
        t1 = time.time()
        cur.execute(sql)
        rows = cur.fetch_arrow_table().to_pandas()
        t2 = time.time()
        rows = rows.to_dict("records")
        return rows, t2 - t1


class IbisRunner(Runner):
    def setup(self, db="tpch.db"):
        super().setup(db=db)
        self.con = getattr(ibis, self.backend).connect(db)

    def run(self, qid, outdir=None, backend="sqlite"):
        import importlib

        mod = importlib.import_module(f".{qid}", package="ibis_tpc")
        q = getattr(mod, f"tpc_{qid}")(self.con)

        out_txt(repr(q), outdir, f"{qid}-{self.interface}-{self.backend}-expr.txt")

        out_txt(
            str(ibis.to_sql(q)),
            outdir,
            f"{qid}-{self.interface}-{self.backend}-compiled.sql",
        )

        t1 = time.time()
        rows = q.execute()
        t2 = time.time()

        return rows.to_dict("records"), t2 - t1


class SubstraitRunner(Runner):
    def setup(self, db="tpch.ddb"):
        self.con = TPCHBackend(fname=db)

    def run(self, qid, outdir=None, backend="substrait"):
        import importlib

        from ibis_substrait.compiler.core import SubstraitCompiler

        compiler = SubstraitCompiler()

        mod = importlib.import_module(f".{qid}", package="ibis_tpc")
        query = getattr(mod, f"tpc_{qid}")(self.con)

        t1 = time.time()
        try:
            proto = compiler.compile(query)
        except Exception:
            raise ValueError("can't compile")

        results = self.con.con.from_substrait(proto.SerializeToString())
        rows = results.to_df()
        t2 = time.time()

        return rows.to_dict("records"), t2 - t1

    def teardown(self):
        self.errors = []


class SqlAlchemyRunner(Runner):
    def setup(self, db="tpch.db"):
        super().setup(db=db)

        from sqlalchemy import MetaData, create_engine

        self.engine = create_engine(f"sqlite:///{db}")
        self.metadata = MetaData(self.engine)

    def table(self, tblname):
        from sqlalchemy import Table

        return Table(tblname, self.metadata, autoload=True)

    def run(self, qid, outdir=None, backend="sqlite"):
        import importlib

        mod = importlib.import_module(f".{qid}", package="sqlalchemy_tpc")
        q = getattr(mod, f"tpc_{qid}")(self)

        sql = q.compile(self.engine, compile_kwargs=dict(literal_binds=True))
        out_txt(sql, outdir, f"{qid}-{self.interface}-{self.backend}-compiled.sql")

        t1 = time.time()
        rows = q.execute()
        t2 = time.time()

        return list(dict(r) for r in rows), t2 - t1


class RRunner(Runner):
    def setup(self, db="tpch.db"):
        super().setup(db=db)
        os.putenv("R_LIBS_SITE", "/usr/lib/R/library")  # skip warnings

        import rpy2
        import rpy2.robjects
        import rpy2.robjects.packages as rpackages
        import rpy2.robjects.pandas2ri

        rpy2.robjects.pandas2ri.activate()

        rpy2.rinterface_lib.callbacks.consolewrite_print = self.print
        rpy2.rinterface_lib.callbacks.consolewrite_warnerror = self.warn

        pkgs = ("dplyr", "dbplyr", "lubridate", "DBI", "RSQLite")
        names_to_install = [x for x in pkgs if not rpackages.isinstalled(x)]

        if names_to_install:
            from rpy2.robjects.vectors import StrVector

            utils = rpackages.importr("utils")
            utils.chooseCRANmirror(ind=1)  # select first mirror in the list
            utils.install_packages(StrVector(names_to_install))

        r = rpy2.robjects.r
        r["source"]("dplyr_tpc/init.R")

        self.query_dbplyr = rpy2.robjects.globalenv["query_dbplyr"]
        self.query_dplyr = rpy2.robjects.globalenv["query_dplyr"]
        self.query_sql = rpy2.robjects.globalenv["query_sql"]

        self.con = rpy2.robjects.globalenv["setup_sqlite"](db)

    def teardown(self):
        super().teardown()
        import rpy2.robjects

        rpy2.robjects.globalenv["teardown_sqlite"](self.con)

    def run(self, qid, outdir=None, backend="sqlite"):
        import rpy2.robjects

        r = rpy2.robjects.r
        fn = f"dplyr_tpc/{qid}.R"

        if not Path(fn).exists():
            raise FileNotFoundError(fn)

        r["source"](fn)
        func = rpy2.robjects.globalenv[f"tpc_{qid}"]

        sql = self.query_sql(self.con, func)[0]
        out_txt(sql, outdir, f"{qid}-{self.interface}-{self.backend}.sql")

        t1 = time.time()
        res = rpy2.robjects.globalenv["query_" + self.interface](self.con, func)
        t2 = time.time()

        return res.to_dict("records"), t2 - t1


setup_sqlite = SqliteRunner
setup_ibis = IbisRunner
setup_sqlalchemy = SqlAlchemyRunner
setup_duckdb = DuckDBRunner
setup_substrait = SubstraitRunner
setup_dplyr = RRunner
setup_dbplyr = RRunner


def compare(rows1, rows2):
    diffs = []
    for rownum, (r1, r2) in enumerate(itertools.zip_longest(rows1, rows2)):
        if r1 is None:
            diffs.append(f"[{rownum}]  extra row: {r2}")
            continue

        if r2 is None:
            diffs.append(f"[{rownum}]  extra row: {r1}")
            continue

        lcr1 = {k.lower(): v for k, v in r1.items()}
        lcr2 = {k.lower(): v for k, v in r2.items()}
        keys = set(lcr1.keys())
        keys |= set(lcr2.keys())
        for k in keys:
            v1 = lcr1.get(k, None)
            v2 = lcr2.get(k, None)
            if diff := _compare(v1, v2, row=rownum, key=k):
                diffs.append(diff)

    return diffs


@dispatch(Decimal, float)
def _compare(v1, v2, row=None, key=None):
    v1 = float(v1)
    return _compare(v1, v2, row=row, key=key)


@dispatch(float, Decimal)
def _compare(v1, v2, row=None, key=None):
    v2 = float(v2)
    return _compare(v1, v2, row=row, key=key)


@dispatch(pandas.Timestamp, pandas.Timestamp)
def _compare(v1, v2, row=None, key=None):
    if v1 != v2:
        return f"[{row}].{key} (date) {v1} != {v2}"


@dispatch(pandas.Timestamp, datetime.date)
def _compare(v1, v2, row=None, key=None):
    if v1.date() != v2:
        return f"[{row}].{key} (date) {v1} != {v2}"


@dispatch(datetime.date, pandas.Timestamp)
def _compare(v1, v2, row=None, key=None):
    if v1 != v2.date():
        return f"[{row}].{key} (date) {v1} != {v2}"


@dispatch(str, pandas.Timestamp)
def _compare(v1, v2, row=None, key=None):  # noqa: F811
    v1 = pandas.to_datetime(v1)
    return _compare(v1, v2, row=row, key=key)


@dispatch(pandas.Timestamp, str)
def _compare(v1, v2, row=None, key=None):  # noqa: F811
    v2 = pandas.to_datetime(v2)
    return _compare(v1, v2, row=row, key=key)


@dispatch(float, float)
def _compare(v1, v2, row=None, key=None):  # noqa: F811
    if not math.isclose(v1, v2, rel_tol=1e-6):
        percent_diff = 100
        if v1:
            percent_diff = abs(v2 - v1) / v1 * 100
        return f"[{row}].{key} (float) {v1} != {v2} ({percent_diff}%)"


@dispatch(float, object)
def _compare(v1, v2, row=None, key=None):  # noqa: F811
    if math.isnan(v1) and v2 is None:
        pass
    elif not math.isnan(v1):
        return f"[{row}].{key} {v1} ({type(v1)}) != {v2} ({type(v2)})"


@dispatch(object, float)
def _compare(v1, v2, row=None, key=None):  # noqa: F811
    if math.isnan(v1) and v2 is None:
        pass
    elif not math.isnan(v2):
        return f"[{row}].{key} {v1} ({type(v1)}) != {v2} ({type(v2)})"


@dispatch(int, float)
def _compare(v1, v2, row=None, key=None):  # noqa: F811
    v1 = float(v1)
    return _compare(v1, v2, row=row, key=key)


@dispatch(float, int)
def _compare(v1, v2, row=None, key=None):  # noqa: F811
    v2 = float(v2)
    return _compare(v1, v2, row=row, key=key)


@dispatch(object, object)
def _compare(v1, v2, row=None, key=None):  # noqa: F811
    if v1 != v2:
        return f"[{row}].{key} {v1} ({type(v1)}) != {v2} ({type(v2)})"


@click.command()
@click.argument("qids", nargs=-1)
@click.option(
    "-d",
    "--db",
    default="tpch.db",
    help="connection string for db to run queries against",
)
@click.option("-b", "--backend", default="sqlite", help="backend to use with given db")
@click.option(
    "-i",
    "--interface",
    "interfaces",
    multiple=True,
    help="interface to use with backend: sqlite|ibis|dplyr|dbplyr",
)
@click.option(
    "-o",
    "--output",
    "outdir",
    type=click.Path(),
    default=None,
    help="directory to save intermediate and debug outputs",
)
@click.option("-v", "--verbose", count=True, help="display more information on stdout")
@click.option("--debug", is_flag=True, help="abort on error and print backtrace")
def main(qids, db, outdir, interfaces, backend, verbose, debug):
    if outdir:
        os.makedirs(outdir, exist_ok=True)
        try:
            os.remove(Path(outdir) / "benchmarks.jsonl")
        except FileNotFoundError:
            pass
        try:
            os.remove(Path(outdir) / "benchmarks.txt")
        except FileNotFoundError:
            pass

    global g_debug
    g_debug = debug

    if not qids:
        qids = sorted(
            list(
                set(Path(fn).stem for fn in glob.glob("sqlite_tpc/*.sql") if "." in fn)
            )
        )

    if not interfaces:
        interfaces = ["sqlite", "ibis", "dplyr", "dbplyr"]

    runners = [
        globals()["setup_" + interface](interface=interface, backend=backend)
        for interface in interfaces
    ]

    nerrs = 0
    ndiffs = 0

    for qid in qids:
        results = []
        for runner, interface in zip(runners, interfaces):
            runner.setup(db)

            info = dict(qid=qid)
            info.update(runner.info())

            try:
                rows, elapsed_s = runner.run(qid, backend=backend, outdir=outdir)
                out_jsonl(rows, outdir, f"{qid}-{interface}-{backend}-results.jsonl")

                # No output is a failure
                if not len(rows):
                    raise ValueError("Empty output (zero rows)")

                info["nrows"] = len(rows)
                info["elapsed_s"] = elapsed_s

                # first interface is baseline for correctness
                if results:
                    diffs = compare(results[0], rows)
                    out_txt(
                        "\n".join(diffs),
                        outdir,
                        f"{qid}-{interface}-{backend}-diffs.txt",
                    )
                    info["ndiffs"] = len(diffs)
                    ndiffs += len(diffs)

                results.append(rows)
            except KeyboardInterrupt:
                return
            except Exception as e:
                if g_debug:
                    raise
                rows = []
                runner.error(type(e).__name__ + ": " + str(e))

            if runner.errors:
                info["errors"] = "; ".join(runner.errors)
                nerrs += len(runner.errors)

            if verbose > 0 and runner.warns:
                info["warns"] = "; ".join(runner.warns)

            if verbose > 1 and runner.prints:
                info["prints"] = "; ".join(runner.prints)

            if outdir:
                with open(Path(outdir) / "benchmarks.txt", mode="a") as fp:
                    print("  ".join(f"{k}:{fmt(v)}" for k, v in info.items()), file=fp)

                with open(Path(outdir) / "benchmarks.jsonl", mode="a") as fp:
                    print(json.dumps(info), file=fp)

            print("  ".join(f"{k}:{fmt(v)}" for k, v in info.items()))

            runner.teardown()

    if nerrs > 0 or ndiffs > 0:
        raise click.ClickException(f"{nerrs} errors, {ndiffs} diffs")


if __name__ == "__main__":
    main()
