#!/usr/bin/env python3

import itertools
import json
import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Any

import click
import pandas
import ibis


def write_stdout(s):
    print(s, end='')


def write_stderr(s):
    print(s, end='', file=sys.stderr)


def out_txt(s, outdir, fn):
    if outdir:
        print(s, file=open(Path(outdir)/fn, mode='w'), flush=True)


def out_sql(sql, outdir, fn):
    import sqlparse
    sql = sqlparse.format(str(sql), reindent=True, keyword_case='upper')

    out_txt(sql, outdir, fn)


def out_jsonl(rows: List[Dict[str, Any]], outdir, fn):
    class DateEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, pandas.Timestamp):
                return str(obj)
            return json.JSONEncoder.default(self, obj)

    if outdir:
        with open(Path(outdir)/fn, mode='w') as fp:
            for r in rows:
                print(json.dumps(r, cls=DateEncoder), file=fp, flush=True)


def out_benchmark(**kwargs):
    def fmt(v):
        if isinstance(v, float):
            return '%.03f' % v
        else:
            return str(v)

    print('  '.join(f'{k}:{fmt(v)}' for k, v in kwargs.items()))


def setup_sqlite(db='tpch.db'):
    import sqlite3

    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row

    return con


def teardown_sqlite(con):
    pass


def run_sqlite(con, qid, outdir=None, backend='sqlite'):
    cur = con.cursor()

    try:
        sql = open(f'queries/{qid}.sql').read()
        t1 = time.time()
        cur.execute(sql)
        rows = cur.fetchall()
        t2 = time.time()
        rows = list(dict(r) for r in rows)
        out_jsonl(rows, outdir, f'{qid}-{backend}.jsonl')
        return rows, t2-t1
    except FileNotFoundError:
        return []


def setup_ibis(db='tpch.db', backend='sqlite'):
    return getattr(ibis, backend).connect(db)


def teardown_ibis(con):
    pass


def run_ibis(con, qid, outdir=None, backend='sqlite'):
    import importlib
    mod = importlib.import_module(f'queries.{qid}')
    q = getattr(mod, f'tpc_{qid}')(con)

    out_txt(repr(q), outdir, f'{qid}-ibis-repr.txt')

    out_sql(str(q.compile()), outdir, f'{qid}-ibis-{backend}.sql')

    t1 = time.time()
    rows = q.execute()
    t2 = time.time()

    out_jsonl(rows, outdir, f'{qid}-ibis-{backend}.jsonl')
    return rows.to_dict('records'), t2-t1


def setup_r(db='tpch.db', backend='sqlite', quiet=False):
    os.putenv('R_LIBS_SITE', '/usr/lib/R/library')  # skip warnings

    import rpy2
    import rpy2.robjects
    import rpy2.robjects.pandas2ri
    import rpy2.robjects.packages as rpackages

    rpy2.robjects.pandas2ri.activate()

    if quiet:
        rpy2.rinterface_lib.callbacks.consolewrite_print = lambda s: None
        rpy2.rinterface_lib.callbacks.consolewrite_warnerror = lambda s: None
    else:
        rpy2.rinterface_lib.callbacks.consolewrite_print = write_stdout
        rpy2.rinterface_lib.callbacks.consolewrite_warnerror = write_stderr

    pkgs = ('dplyr', 'dbplyr', 'lubridate', 'DBI', 'RSQLite')
    names_to_install = [x for x in pkgs if not rpackages.isinstalled(x)]

    if names_to_install:
        from rpy2.robjects.vectors import StrVector
        utils = rpackages.importr('utils')
        utils.chooseCRANmirror(ind=1)  # select the first mirror in the list
        utils.install_packages(StrVector(names_to_install))

    r = rpy2.robjects.r
    r['source']('init.R')

    global query_dbplyr, query_dplyr, query_sql
    query_dbplyr = rpy2.robjects.globalenv['query_dbplyr']
    query_dplyr = rpy2.robjects.globalenv['query_dplyr']
    query_sql = rpy2.robjects.globalenv['query_sql']

    return rpy2.robjects.globalenv['setup_sqlite'](db)


def teardown_r(con):
    import rpy2.robjects
    rpy2.robjects.globalenv['teardown_sqlite'](con)


def run_r(con, qid, outdir=None, backend='sqlite', queryfunc=None):
    import rpy2.robjects

    r = rpy2.robjects.r
    fn = f'queries/{qid}.R'
    if not Path(fn).exists():
        return []

    r['source'](fn)
    func = rpy2.robjects.globalenv[f'tpc_{qid}']

    sql = query_sql(con, func)
    out_sql(sql, outdir, f'{qid}-r-{backend}.sql')

    t1 = time.time()
    res = queryfunc(con, func)
    t2 = time.time()

    rows = res.to_dict('records')
    out_jsonl(rows, outdir, f'{qid}-{backend}-r.jsonl')
    return rows, t2-t1


def compare(rows1, rows2):
    diffs = []
    for i, (r1, r2) in enumerate(itertools.zip_longest(rows1, rows2)):
        if r1 is None:
            diffs.append(f'[{i}]  extra row: {r2}')
            continue

        if r2 is None:
            diffs.append(f'[{i}]  extra row: {r1}')
            continue

        keys = set(r1.keys())
        keys |= set(r2.keys())
        for k in keys:
            v1 = r1.get(k, None)
            v2 = r2.get(k, None)
            if 'DATE' in k:
                if not v2 or v1 != v2.strftime('%Y-%m-%d'):
                    diffs.append(f'[{i}].{k} {v1} ({type(v1)} != {v2} ({type(v2)}')
            else:
                if v1 != v2:
                    diffs.append(f'[{i}].{k} {v1} ({type(v1)} != {v2} ({type(v2)}')

    return diffs


@click.command()
@click.argument('qids', nargs=-1)
@click.option('--db', default='tpch.db', help='db to run against')
@click.option('--outdir', type=click.Path(), default=None, help='')
@click.option('--backend', default='sqlite', help='ibis backend to use')
def main(qids, db, outdir, backend):
    kwargs = dict(outdir=outdir, backend=backend)
    con1 = setup_sqlite(db)
    con2 = setup_ibis(db, backend=backend)
    con3 = setup_r(db, backend=backend)

    for qid in qids:
        rows1, t1 = run_sqlite(con1, qid, **kwargs)
        out_benchmark(qid=qid, method='raw-sqlite',
                      nrows=len(rows1), ndiffs=0, elapsed_s=t1)

        rows2, t2 = run_ibis(con2, qid, **kwargs)
        diffs2 = compare(rows1, rows2)
        out_benchmark(qid=qid, method='ibis-sqlite',
                      nrows=len(rows2), ndiffs=len(diffs2), elapsed_s=t2)
        out_txt('\n'.join(diffs2), outdir, f'{qid}-ibis-diffs.txt')

        rows3, t3 = run_r(con3, qid, queryfunc=query_dplyr, **kwargs)
        diffs3 = compare(rows1, rows3)
        out_benchmark(qid=qid, method='dplyr-sqlite',
                      nrows=len(rows3), ndiffs=len(diffs3), elapsed_s=t3)
        out_txt('\n'.join(diffs3), outdir, f'{qid}-dplyr-diffs.txt')

        rows4, t4 = run_r(con3, qid, queryfunc=query_dbplyr, **kwargs)
        diffs4 = compare(rows1, rows4)
        out_benchmark(qid=qid, method='dbplyr-sqlite',
                      nrows=len(rows4), ndiffs=len(diffs4), elapsed_s=t4)
        out_txt('\n'.join(diffs4), outdir, f'{qid}-dbplyr-diffs.txt')

    teardown_sqlite(con1)
    teardown_ibis(con2)
    teardown_r(con3)


if __name__ == '__main__':
    main()
