# DuckDB 2.0: agent mode and new SQL

What DuckDB 2.0.0 adds that matters for querying here. Every example below was run on the local
2.0.0 shell; `.help agent` in the shell is the authoritative summary.

## Agent mode

### When it is on

On by itself when an agent's environment variable is set (Claude Code sets `CLAUDECODE` /
`AI_AGENT`), stdout is not a terminal, and no output-mode flag is given. So every `duckdb` call
from the Bash tool is in agent mode unless told otherwise.

| Control | Effect |
| --- | --- |
| `-agent` / `-no-agent` | Force on / off; they beat the environment |
| `DUCKDB_AGENT_MODE=1` / `0` | Force on / off from the environment |
| `-csv`, `-json`, `-markdown`, ... | A format flag is a deliberate choice: agent mode off |
| `.show` | Prints `mode` and `agent: claude-code_...` when on |

It prints one startup line on stderr (`duckdb agent mode on (...)`); ignore it.

### What changes

- **Tables:** compact markdown, each header typed: `| a:INTEGER | b:VARCHAR |`.
- **Footer:** row count, plus an order-independent hash of the whole result, e.g.
  `12 rows, hash 3f667ed397940a9e`. It appears for empty results, results of 10+ rows, and capped
  ones. It tells NULL from the string `'NULL'`.
- **Caps:** a result up to 1,000 rows and 10,000 bytes prints whole; a larger one prints its first
  and last 20 rows, with a footer such as `first 20 and last 20 of 100000 rows (.maxrows -1 for
  all), hash ...`. Cells are cut at 500 characters.
- **Early stop:** after the cap, rows are only counted; 100,000 rows later the query is stopped.
  `FROM range(1_000_000_000)` returns at once with
  `first 20 of at least 102400 rows (query stopped early; .maxrows -1 for all)`, and no hash.
- **Estimates:** a statement expected to read 1,000,000+ rows first prints, on stderr,
  `estimate: ~5000002 rows read (range ~5000000, range ~2), ~1 rows returned`. Queries running
  longer print `progress: 42% (elapsed 12.0s, remaining ~16.5s)` every 5 seconds.
- **Errors:** JSON on stderr with `exception_type`, `exception_message`, `error_subtype`,
  `position`, `location`, and the offending `name`:
  `{"exception_type":"Binder","exception_message":"Referenced column \"nope\" not found ...",
  "error_subtype":"COLUMN_NOT_FOUND","position":"7","location":"[7,4]","name":"nope"}`.
- **`.tables`:** one line per table with approximate rows, types and keys:
  `memory.main.t1 (table, ~15 rows): a INTEGER PK, b VARCHAR`.
- **`EXPLAIN`:** compact, one operator per line, indented by depth:
  `HASH_JOIN (est=10) Join Type: INNER; Conditions: CAST(a AS BIGINT) = range`. Outside agent mode,
  `EXPLAIN (FORMAT compact)` gives the same.

| Dot command | Sets (agent mode) |
| --- | --- |
| `.maxrows N` | Rows rendered whole (`-1` = all; in box mode the default is 40) |
| `.maxbytes N` | Bytes rendered whole (`0` = all) |
| `.maxcellwidth N` | Characters per cell (`0` = all) |

### How to use it here

- **Explore in agent mode, show in box mode.** Work out a query with agent mode on (it is cheap to
  read, cannot flood output, and stops runaways early, so the file is released sooner). Run the
  final query with `-no-agent` and paste that box table for the user.
- **Check before running:** `DESCRIBE SELECT ...` gives the result's columns without running it;
  an `estimate:` line over a few million rows on a large table means narrow the query first.
- **Bound the time:** `SET max_execution_time = 30000;` (milliseconds) at the start of a script,
  as well as `timeout 60`. Over the limit, the query fails with
  `{"exception_type":"INTERRUPT","exception_message":"Query exceeded maximum execution time"}`.
- **Verify a rewrite with the hash:** when refactoring a query (a cast, a join, a dedupe), the same
  hash before and after means the same rows, in any order, without printing either result.
  `count(*) ... GROUP BY 1 ORDER BY 1` and `count(_dlt_id) ... GROUP BY ALL ORDER BY n` on the
  same table give the same hash.
- **Read the JSON error:** `name` and `location` point at the token to fix.
- **Look functions up:** `SELECT function_name, description, examples FROM duckdb_functions()
  WHERE function_name ILIKE '%json%';` (about two thirds of functions have a description).

## New SQL in 2.0

| Feature | Example |
| --- | --- |
| `$variables` | `SET VARIABLE cutoff = DATE '2026-10-10'; SELECT $cutoff;` |
| `FETCH FIRST` | `SELECT * FROM t ORDER BY x DESC FETCH FIRST 2 ROWS ONLY;` |
| `VARIANT`, a binary type for JSON-shaped data, stored as columns | `'{"user": {"id": 42}}'::JSON::VARIANT`; `doc.user.id`, `variant_type(doc)`, `variant_keys(doc)` |
| JSON edits | `json_set(j, '$.b', '2'::JSON)` (the value must be JSON), `json_insert`, `json_replace`, `json_remove(j, '$.a')` |
| Nearest joins | `FROM q INNER JOIN emb NEAREST 2 BY DISTANCE array_distance(q.e, emb.e)`; `BY SIMILARITY` is higher-is-closer; `APPROX NEAREST` is faster on large inputs; works with `LEFT JOIN` |
| Data-modifying CTEs | `WITH moved AS (DELETE FROM t WHERE ... RETURNING *) SELECT count(*) FROM moved;` |
| Triggers | `CREATE TRIGGER log AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.id, 'insert');`, `BEFORE`/`AFTER`, `FOR EACH ROW`/`STATEMENT`, `DROP TRIGGER` |
| Nested schemas | `CREATE SCHEMA finance.reports;` (the parent must exist) |
| `UNNEST` in `GROUP BY`; defined `MERGE` / `UPDATE ... FROM` with several matching rows | |

Triggers, data-modifying CTEs and new tables are writes: only on request, never on loader-managed
tables, and never in a database a pipeline writes to (see the version rule in `SKILL.md`).

## Breaking changes that affect queries

- **Lambdas:** `lambda x: x + 1`. `x -> x + 1` is a Binder error (it can be re-enabled with
  `SET lambda_syntax = 'ENABLE_SINGLE_ARROW'`, removed in 2.1).
- **Parser:** a new PEG parser; error messages carry exact positions. If a query that worked in 1.5
  fails to parse, the old parser is in the `legacy_parser` extension (`SET enable_legacy_parser =
  true;`).
- **Storage:** a file created by 2.0 cannot be opened by 1.5 (the pipelines' version).

## Also new, not used here

- **Client/server (Quack):** `duckdb -serve` / `CALL quack_serve(token = '...')` serves a database;
  `ATTACH 'quack:localhost' AS qk (TOKEN '...'); CONNECT qk;` uses it. `CONNECT` also switches to
  Postgres and MySQL, with queries pushed down to them. Localhost only by default.
- **Asynchronous I/O:** faster Parquet, CSV and S3 reads, with no changes needed.
- **Python client:** streams results; faster export to polars, pandas and Arrow; Python 3.11+.
- **Extensions:** C API v2, self-hosted repositories (`CREATE EXTENSION REPOSITORY`), DuckLake 1.1,
  and Iceberg and Delta updates.

Sources: [2.0 announcement](https://duckdb.org/2026/10/21/announcing-duckdb-200),
[agent mode docs](https://duckdb.org/docs/2.0/clients/cli/agent_mode),
[agent mode post](https://duckdb.org/2026/10/09/agent-mode),
[FROM and joins](https://duckdb.org/docs/2.0/sql/query_syntax/from).
