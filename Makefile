DB ?= tpch.db

SQL_QUERIES= $(wildcard queries/*.sql)
PY_QUERIES= $(wildcard queries/*.py)

all: golden-py

golden-sql: $(SQL_QUERIES:.sql=-sql.json)

golden-py: $(PY_QUERIES:.py=-py.json)

%-sql.json: %.sql init.sql
	cat $< | sqlite3 -init init.sql ${DB} > $@

%-py.json: %.py runner.py
	./runner.py --db ${DB} --outjson $@ $<
