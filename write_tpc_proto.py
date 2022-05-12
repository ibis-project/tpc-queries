import importlib

import click
from ibis_substrait.compiler.core import SubstraitCompiler

from ibis_tpc.tests.conftest import TPCHBackend


@click.command()
@click.option("--query", required=False, type=str, help="TPC query, e.g. 'h01'")
@click.option(
    "--all",
    "_all",
    default=True,
    type=bool,
    help="Write all TPC-H queries to substrait_tpc directory",
)
def write_queries(query, _all):
    con = TPCHBackend()

    if query:
        write_query(con, query)
    elif _all:
        for i in range(1, 23):
            write_query(con, f"h{i:02d}")
    else:
        raise ValueError("No query given")


def write_query(con, query):
    module = importlib.import_module(f"ibis_tpc.{query}")
    func = getattr(module, f"tpc_{query}")

    expr = func(con)

    compiler = SubstraitCompiler()
    try:
        plan = compiler.compile(expr)
    except Exception:
        print(f"Failed to compile query {query}")
        return

    with open(f"substrait_tpc/tpc_{query}.bin", "wb") as f:
        f.write(plan.SerializeToString())


if __name__ == "__main__":
    write_queries()
