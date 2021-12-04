#!/usr/bin/env python3

# Usage: $0 tpch.db q01

import sys

import ibis


def main(dbfn, qid='q01'):
    pycode = open(f'queries/{qid}.py').read()

    try:
        exec(pycode, globals())
    except Exception as e:
        import traceback
        traceback.print_exception(e, file=sys.stderr)

    func = globals()[f'tpch_{qid}']

    con = ibis.sqlite.connect(dbfn)
    q = func(con)

    print(repr(q), file=sys.stderr)

    sql = q.compile()
    try:
        import sqlparse
        sql = sqlparse.format(str(sql), reindent=True, keyword_case='upper')
    except ModuleNotFoundError as e:
        print(e, file=sys.stderr)

    print(sql, file=sys.stderr)

    r = q.execute()
    print(r.to_json(orient='records'))


if __name__ == '__main__':
    main(*sys.argv[1:])
