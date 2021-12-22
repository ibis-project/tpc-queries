# TPC query translations

All queries are ready-to-run, using the default substitution parameters as specified by the TPC-H spec.

- Each translation of each query is in its own file in the `queries/` directory.
  - .sql files are taken directly from the spec and modified as needed to be executed directly by SQLite
  - .R files are written in R, from [ursacomputing/arrowbench](https://github.com/ursacomputing/arrowbench/blob/main/R/tpch-queries.R)
  - .py files are translated to [Ibis](https://github.com/ibis-project/ibis)

## Usage

```
./tpc.py [--db <tpch.sqlite>] [--output <output_dir>] [-v] <queries>
```

where:
- `<output_dir>` specifies a directory for debugging files: results, generated queries, logs (default is no debugging output)
- `<tpch.sqlite>` is the TPC database to use
- `<queries>` is any number of TPC query ids: `h01` .. `h22`


## Notes

- Some queries require a TPC database with scale factor of .01 at minimum to generate results.
- Use this [sqlite db with a scaling factor of .01](https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H-small.db) (10MB).

## Links
- [TPC-H v3.0 specification](http://tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.0.pdf)
