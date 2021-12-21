#!/usr/bin/env python3

import glob
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


def out_benchmark(outdir, **kwargs):
    def fmt(v):
        if isinstance(v, float):
            return '%.03f' % v
        else:
            return str(v)

    if outdir:
        with open(Path(outdir)/'benchmarks.txt', mode='a') as fp:
            print('  '.join(f'{k}:{fmt(v)}' for k, v in kwargs.items()), file=fp)
        with open(Path(outdir)/'benchmarks.jsonl', mode='a') as fp:
            print(json.dumps(kwargs), file=fp)

#        if not quiet:
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
        return rows, dict(elapsed_s=t2-t1, nrows=len(rows))
    except Exception as e:
        return [], {'error': str(e)}


def setup_ibis(db='tpch.db', backend='sqlite'):
    return getattr(ibis, backend).connect(db)


def teardown_ibis(con):
    pass


def run_ibis(con, qid, outdir=None, backend='sqlite'):
    import importlib
    mod = importlib.import_module(f'queries.{qid}')
    q = getattr(mod, f'tpc_{qid}')(con)

    out_txt(repr(q), outdir, f'{qid}-ibis-repr.txt')

    out_sql(q.compile(), outdir, f'{qid}-ibis-{backend}.sql')

    t1 = time.time()
    rows = q.execute()
    t2 = time.time()

    outrows = rows.to_dict('records')
    out_jsonl(outrows, outdir, f'{qid}-ibis-{backend}.jsonl')
    return outrows, dict(elapsed_s=t2-t1, nrows=len(outrows))


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
        return [], {}

    r['source'](fn)
    func = rpy2.robjects.globalenv[f'tpc_{qid}']

    sql = query_sql(con, func)[0]
    out_sql(sql, outdir, f'{qid}-r-{backend}.sql')

    errors = []
    rows = []
    t1 = time.time()
    try:
        res = queryfunc(con, func)
        rows = res.to_dict('records')
        out_jsonl(rows, outdir, f'{qid}-{backend}-r.jsonl')
    except rpy2.rinterface_lib.embedded.RRuntimeError as e:
        errors.append(str(e))

    t2 = time.time()

    return rows, dict(elapsed_s=t2-t1,
                      nrows=len(rows),
                      errors='\n'.join(errors))


def compare(rows1, rows2):
    diffs = []
    for i, (r1, r2) in enumerate(itertools.zip_longest(rows1, rows2)):
        if r1 is None:
            diffs.append(f'[{i}]  extra row: {r2}')
            continue

        if r2 is None:
            diffs.append(f'[{i}]  extra row: {r1}')
            continue

        lcr1 = {k.lower(): v for k, v in r1.items()}
        lcr2 = {k.lower(): v for k, v in r2.items()}
        keys = set(lcr1.keys())
        keys |= set(lcr2.keys())
        for k in keys:
            v1 = lcr1.get(k, None)
            v2 = lcr2.get(k, None)
            if isinstance(v2, pandas.Timestamp):
                if v1 != v2.strftime('%Y-%m-%d'):
                    diffs.append(f'[{i}].{k} (date) {v1} != {v2}')
            elif isinstance(v1, float) and isinstance(v2, float):
                if v2 != v1:
                    if v1:
                        dv = abs(v2 - v1)
                        pd = dv/v1
                    else:
                        pd = 1
                    if pd > 1e-10:
                        diffs.append(f'[{i}].{k} (float) {v1} != {v2} ({pd*100}%)')

            else:
                if v1 != v2:
                    diffs.append(f'[{i}].{k} {v1} ({type(v1)}) != {v2} ({type(v2)})')

    return diffs


@click.command()
@click.argument('qids', nargs=-1)
@click.option('--db', default='tpch.db', help='db to run against')
@click.option('--outdir', type=click.Path(), default=None, help='')
@click.option('--backend', default='sqlite', help='ibis backend to use')
def main(qids, db, outdir, backend):
    kwargs = dict(outdir=outdir)
    if outdir:
        os.makedirs(outdir, exist_ok=True)
        try:
            os.remove(Path(outdir)/'benchmarks.jsonl')
        except FileNotFoundError:
            pass
        try:
            os.remove(Path(outdir)/'benchmarks.txt')
        except FileNotFoundError:
            pass

    if not qids:
        qids = sorted(list(set(Path(fn).stem for fn in glob.glob('queries/*.sql') if '.' in fn)))

    con1 = setup_sqlite(db)
    con2 = setup_ibis(db, backend=backend)
    con3 = setup_r(db, backend=backend)

    for qid in qids:
        rows1, info = run_sqlite(con1, qid, backend='raw-sqlite', **kwargs)
        out_benchmark(outdir, qid=qid, method='raw-sqlite',
                      ndiffs=0, **info)

        try:
            rows2, info = run_ibis(con2, qid, backend='ibis-sqlite', **kwargs)
            diffs2 = compare(rows1, rows2)
            out_txt('\n'.join(diffs2), outdir, f'{qid}-ibis-diffs.txt')
        except Exception as e:
            rows2 = []
            diffs2 = []
            info = {'errors': str(e)}

        out_benchmark(outdir, qid=qid, method='ibis-sqlite', ndiffs=len(diffs2), **info)

        rows3, info = run_r(con3, qid, queryfunc=query_dplyr, backend='dplyr', **kwargs)
        diffs3 = compare(rows1, rows3)
        out_benchmark(outdir, qid=qid, method='dplyr-sqlite',
                      ndiffs=len(diffs3), **info)
        out_txt('\n'.join(diffs3), outdir, f'{qid}-dplyr-diffs.txt')

        rows4, info = run_r(con3, qid, queryfunc=query_dbplyr, backend='dbplyr', **kwargs)
        diffs4 = compare(rows1, rows4)
        out_benchmark(outdir, qid=qid, method='dbplyr-sqlite',
                      ndiffs=len(diffs4), **info)
        out_txt('\n'.join(diffs4), outdir, f'{qid}-dbplyr-diffs.txt')

    teardown_sqlite(con1)
    teardown_ibis(con2)
    teardown_r(con3)


if __name__ == '__main__':
    main()
