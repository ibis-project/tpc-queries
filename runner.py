#!/usr/bin/env python3

import itertools
import json
import time
from pathlib import Path
from typing import List, Dict, Any

import click
import pandas
import ibis


def out_txt(s, outdir, fn):
    if outdir:
        print(s, file=open(Path(outdir)/fn, mode='w'), flush=True)


def run_sqlite(qid, db='tpch.db', outdir=None):
    import sqlite3

    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    try:
        sql = open(f'queries/{qid}.sql').read()
        cur.execute(sql)
        rows = cur.fetchall()
        return list(dict(r) for r in rows)
    except FileNotFoundError:
        return []


def run_ibis(qid, db='tpch.db', outdir=None, backend='sqlite'):
    import importlib
    mod = importlib.import_module(f'queries.{qid}')
    con = getattr(ibis, backend).connect(db)
    q = getattr(mod, f'tpc_{qid}')(con)

    out_txt(repr(q), outdir, f'{qid}-ibis-repr.txt')

    import sqlparse
    sql = sqlparse.format(str(q.compile()),
                          reindent=True,
                          keyword_case='upper')

    out_txt(sql, outdir, f'{qid}-ibis-{backend}.sql')

    rows = q.execute()
    return rows.to_dict('records')


def setup_r():
    import rpy2
    import rpy2.robjects
    import rpy2.robjects.packages as rpackages

#    rpy2.rinterface_lib.callbacks.consolewrite_print = lambda s: print(s, end='')
#    rpy2.rinterface_lib.callbacks.consolewrite_warnerror = lambda s: print(s, end='')
    rpy2.rinterface_lib.callbacks.consolewrite_print = lambda s: None
    rpy2.rinterface_lib.callbacks.consolewrite_warnerror = lambda s: None

    pkgs = ('dplyr', 'lubridate', 'DBI', 'RSQLite')
    names_to_install = [x for x in pkgs if not rpackages.isinstalled(x)]

    if names_to_install:
        from rpy2.robjects.vectors import StrVector
        utils = rpackages.importr('utils')
        utils.chooseCRANmirror(ind=1)  # select the first mirror in the list
        utils.install_packages(StrVector(names_to_install))

    r = rpy2.robjects.r
    r['source']('init.R')


def run_r(qid, db='tpch.db', outdir=None):
    import rpy2.robjects
    r = rpy2.robjects.r
    fn = f'queries/{qid}.R'
    if not Path(fn).exists():
        return []

    r['source'](fn)

    func = rpy2.robjects.globalenv[f'tpc_{qid}']
    runner = rpy2.robjects.globalenv['run_query']

    return runner(func, db)


def compare(rows1, rows2):
    ndiffs = 0
    for i, (r1, r2) in enumerate(itertools.zip_longest(rows1, rows2)):
        if r1 is None:
            print(f'[{i}]  extra row: {r2}')
            ndiffs += 1
            continue

        if r2 is None:
            print(f'[{i}]  extra row: {r1}')
            ndiffs += 1
            continue

        keys = set(r1.keys())
        keys |= set(r2.keys())
        for k in keys:
            v1 = r1.get(k, None)
            v2 = r2.get(k, None)
            if 'DATE' in k:
                if v1 != v2.strftime('%Y-%m-%d'):
                    print(f'[{i}].{k} {v1} ({type(v1)} != {v2} ({type(v2)}')
                    ndiffs += 1
            else:
                if v1 != v2:
                    print(f'[{i}].{k} {v1} ({type(v1)} != {v2} ({type(v2)}')
                    ndiffs += 1
    return ndiffs


def out_jsonl(p: Path, rows: List[Dict[str, Any]]):
    class DateEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, pandas.Timestamp):
                return str(obj)
            return json.JSONEncoder.default(self, obj)
    with open(p, mode='w') as fp:
        for r in rows:
            print(json.dumps(r, cls=DateEncoder), file=fp, flush=True)


@click.command()
@click.argument('qids', nargs=-1)
@click.option('--db', default='tpch.db', help='db to run against')
@click.option('--outdir', type=click.Path(), default=None, help='')
@click.option('--backend', default='sqlite', help='ibis backend to use')
def main(qids, db, outdir, backend):
    setup_r()
    for qid in qids:
        kwargs = dict(db=db, outdir=outdir)
        t1 = time.time()
        rows1 = run_sqlite(qid, **kwargs)
        t2 = time.time()
        rows2 = run_ibis(qid, **kwargs)
        t3 = time.time()
        rows3 = run_r(qid, **kwargs)
        t4 = time.time()

        if outdir:
            out_jsonl(Path(outdir)/f'{qid}-{backend}.jsonl', rows1)
            out_jsonl(Path(outdir)/f'{qid}-{backend}-ibis.jsonl', rows2)
            out_jsonl(Path(outdir)/f'{qid}-{backend}-r.jsonl', rows3)

        ndiffs = compare(rows1, rows2)

        # r = dict(qid=qid, ndiffs=ndiffs, sqlite_secs=t2-t1, ibis=t3-t2)
        print(f'{qid}  nrows:{len(rows1)}  ndiffs:{ndiffs}  sqlite:{t2-t1:.03f}s  ibis:{t3-t2:.03f}s  r:{t4-t3:.03f}s')


if __name__ == '__main__':
    main()
