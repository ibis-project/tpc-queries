# Usage: make output/q02.diff

DB ?= tpch.db
OUTDIR=output

SQL_OUTS=$(patsubst queries/%.sql,${OUTDIR}/%-sql.jsonl, $(wildcard queries/*.sql))
PY_OUTS=$(patsubst queries/%.py,${OUTDIR}/%-py.jsonl, $(wildcard queries/*.py))

TPCH_DIFFS=$(patsubst queries/%.sql,${OUTDIR}/%.diff, $(wildcard queries/*.sql))

all: golden-diffs

golden-diffs: $(TPCH_DIFFS)
golden-sql: $(SQL_OUTS)
golden-py: $(PY_OUTS)

${OUTDIR}/%.diff: ${OUTDIR}/%-sql.jsonl ${OUTDIR}/%-py.jsonl
	-diff $^ > $@

${OUTDIR}/%-sql.json: queries/%.sql init.sql
	@mkdir -p ${OUTDIR}
	cat $< | time sqlite3 -init init.sql ${DB} > $@

${OUTDIR}/%-py.json: queries/%.py runner.py
	@mkdir -p ${OUTDIR}
	time ./runner.py --db ${DB} --outjson $@ --outsql ${OUTDIR}/`basename $<`.sql --outrepr ${OUTDIR}/`basename $<`.repr $<

%.jsonl: %.json
	cat $< | jq -S -c '.[]' > $@

clean:
	rm -rf ${OUTDIR}/

.PHONY: clean
