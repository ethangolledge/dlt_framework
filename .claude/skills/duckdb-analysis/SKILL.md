---
name: duckdb-analysis
description: Query and analyse data with DuckDB from the shell - a .duckdb database in the local folder, or CSV, Parquet, and JSON files - for any purpose (exploring, profiling, counting, joining, checking quality, answering questions, exporting results). Use whenever the user asks to look at, query, analyse, or run SQL on local data, however it was loaded.
---

# DuckDB analysis

Run SQL with the DuckDB shell (`duckdb`) on local data and report what it shows.

## Which database

**The database is a `.duckdb` file in the local folder: the current working directory and the
folders under it.** Never query a database outside it unless the user gives that path.

1. Find the candidates:
   ```bash
   find . -name '*.duckdb' -not -path './.venv/*' -not -path './.git/*'
   ```
2. Choose one:
   - the file the user names;
   - otherwise, if there is exactly one, that one;
   - otherwise ask which.

   In this repo the pipelines' warehouse is `test_data/warehouse.duckdb` (set by
   `[destination.duckdb] credentials` in `.dlt/secrets.toml`).
3. **Name the file you queried in every answer**, e.g. "from `test_data/warehouse.duckdb`".

Files (CSV, Parquet, JSON) need no database: query them by path, also from the local folder.

## Running SQL

```bash
timeout 60 duckdb -no-agent -readonly <file>.duckdb -c "SELECT ..."       # one or more statements
timeout 60 duckdb -no-agent -readonly <file>.duckdb < queries.sql         # a longer script
timeout 60 duckdb -no-agent -c "SELECT * FROM 'data/*.parquet' LIMIT 10"  # files, no database
```

- **Versions.** The shell is DuckDB 2.0 (`~/.local/bin/duckdb`); the project's Python, which the
  pipelines write with, is 1.5 (capped in `pyproject.toml` until dlt supports 2.0). 2.0 reads and
  writes 1.5 files without converting them, but **a file 2.0 creates cannot be opened by 1.5**:
  never create a new `.duckdb` file with the shell that a pipeline will load into.
- **Agent mode: explore in it, show in box tables.** Under a coding agent the 2.0 shell turns on
  agent mode by itself: compact typed markdown tables, a row-count and result-hash footer, capped
  output, early stop for huge results, cost estimates, and JSON errors. Work a query out without
  `-no-agent`; run the final query with `-no-agent` and show the user that box table. What each
  part means and how to use it (hash checks, `DESCRIBE SELECT`, `max_execution_time`), and the new
  2.0 SQL, are in [references/whats-new-in-2.0.md](references/whats-new-in-2.0.md).

- **Never hold the database open.** Any open connection, even read-only, blocks a pipeline from
  opening the file to write ("Could not set lock on file"), so its run fails. Therefore:
  - one `duckdb` process per query or script, which exits when it ends and releases the file;
  - always wrap it in `timeout 60` so a slow query cannot hold the lock;
  - never an interactive `duckdb` shell, a background process, or a Python session kept open;
  - after a run of queries, check nothing still holds it: `fuser <file>.duckdb` prints nothing.
- **Read only by default.** Open databases with `-readonly`. Write, meaning `CREATE`, `INSERT`,
  `UPDATE`, `DELETE`, `DROP`, or `COPY` into a database, only when the user asks for that change,
  and say what you changed.
- **Leave tables a loading tool manages unchanged** (for dlt, the `raw_*` schemas). For derived
  results, use `CREATE TEMP VIEW` inside the session, or a table the user asks for.
- **One writer at a time.** A file open for writing by another process, such as a running
  pipeline, cannot be opened at all, not even read-only ("Could not set lock on file"). Wait for
  that process to finish; do not delete lock files or copy the database around it.
- **If the shell is unavailable**, use the project's own, closing it in the same command:
  `timeout 60 .venv/bin/python -c "import duckdb; c = duckdb.connect('<file>', read_only=True); print(c.sql('...')); c.close()"`.
- **2.0 syntax.** Lambdas are Python-style, `list_transform(xs, lambda x: x + 1)`; the old
  `x -> x + 1` is rejected. The 1.5 fallback above accepts both.
- **Output format:** the default is DuckDB's box table, which is what the user wants to see. Use
  `-csv`, `-json`, or `-markdown` before `-c` only for machine-readable output or when asked.
- **Look before selecting `*`:** drop dlt's `_dlt_load_id` and `_dlt_id` with
  `SELECT * EXCLUDE (_dlt_load_id, _dlt_id)` unless they matter, and `LIMIT` every preview, so the
  table fits on screen.

## Toolkit

| Need | SQL |
| --- | --- |
| What exists | `SELECT schema_name, table_name, estimated_size FROM duckdb_tables() ORDER BY 1, 2;` |
| A table's columns | `DESCRIBE schema.table;` |
| Profile every column (min, max, nulls, distinct, quartiles) | `SUMMARIZE schema.table;` or `SUMMARIZE SELECT ... ;` |
| Query files | `SELECT * FROM 'path/*.parquet'`, `read_csv('file.csv')`, `read_json('file.json')` |
| Text that should be numbers or dates | `TRY_CAST(...)`, `try_strptime(text, '%-d %b %Y')`; count the nulls a cast produced and report them |
| Duplicates on a key | `SELECT key, count(*) FROM t GROUP BY 1 HAVING count(*) > 1;` |
| Latest row per key | `QUALIFY row_number() OVER (PARTITION BY key ORDER BY loaded_at DESC) = 1` |
| Export a result, when asked | `COPY (SELECT ...) TO 'out.csv' (HEADER);` or `TO 'out.parquet' (FORMAT parquet)` |
| Other databases (SQLite, Postgres) | `ATTACH 'file.db' AS other (TYPE sqlite, READ_ONLY);`; the extension downloads on first use and needs network |

Reserved words used as column aliases, such as `rows`, need quoting: `AS "rows"`.

## Data loaded by dlt

When the database was loaded by this repo's pipelines:

- **One schema per dataset**, named in the pipeline's `Target(dataset=...)`.
- **Ignore `*_staging*` schemas**: they are dlt's working area.
- **Tables starting `_dlt_` are dlt's bookkeeping.**
- **Lists become child tables.** A list field `tags` on `quotes` lands in `quotes__tags`. Join with
  `child._dlt_parent_id = parent._dlt_id`, ordered by `_dlt_list_idx`.
- **Every row has `_dlt_load_id`**, which joins to `_dlt_loads.load_id`: which load wrote it, and
  when.
- **Run monitoring** is Parquet under `monitoring.uri` (locally `test_data/monitoring/`), one folder
  per frame (`runs`, `loads`, `pipelines`, `resources`, `samples`), e.g.
  `SELECT * FROM 'test_data/monitoring/runs/*.parquet' ORDER BY at DESC LIMIT 10`.
- **Raw datasets hold values as extracted.** A scraped `"$1,027.77"` or `"8 Sept 2026"` needs
  casting, and sites spell things their own way: `%b` rejects "Sept". Check every cast's failures
  on the real data before reporting figures. Casts verified on Capitol Trades:
  `TRY_CAST(replace(replace(price, '$', ''), ',', '') AS DECIMAL(12, 2))` and
  `try_strptime(replace(traded, 'Sept', 'Sep'), '%-d %b %Y')`.

## Reporting

- **Show the data.** The user does not see tool output, so paste DuckDB's box table verbatim into
  the reply, in a ```` ```text ```` block, under the SQL that produced it. Never paraphrase rows
  into prose or a rebuilt table; summarise only after showing them.
- Name the database or files you queried.
- State the row counts behind each figure.
- Say when a number depends on a cast, a deduplication, or a sample (e.g. "the newest 60 trades").
- Results go in chat; write a file only when the user asks for an export.
