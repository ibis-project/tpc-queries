DB=tpch.db

all: golden/q01.json

golden/%.json: queries/%.sql
	cat $< | sqlite3 -init init.sql ${DB} > $@
