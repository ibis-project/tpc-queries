#!/usr/bin/env python3

import sys
import click
import sqlparse

import ibis


@click.command()
@click.argument('queryfn', type=click.File('r'))
@click.option('--db', default='tpch.db', help='sqlite db to run against')
@click.option('--outsql', type=click.File('w'), default=sys.stderr,
              help='output filename for compiled SQL')
@click.option('--outrepr', type=click.File('w'), default=sys.stderr,
              help='output filename for ibis query repr')
@click.option('--outjson', type=click.File('w'), default=sys.stdout,
              help='output filename for data results as JSON')
def run(queryfn, db, outsql, outrepr, outjson):
    try:
        exec(queryfn.read(), globals())
    except Exception as e:
        import traceback
        traceback.print_exception(e, file=sys.stderr)

    queries = [func for k, func in globals().items() if k.startswith('query_')]
    if not queries:
        print('no query_ funcs in given .py file', file=sys.stderr)
        return

    con = ibis.sqlite.connect(db)
    for func in queries:
        q = func(con)

        print(repr(q), file=outrepr, flush=True)

        sql = sqlparse.format(str(q.compile()),
                              reindent=True,
                              keyword_case='upper')

        print(sql, file=outsql, flush=True)

        r = q.execute()
        print(r.to_json(orient='records'), file=outjson)

if __name__ == '__main__':
    run()
