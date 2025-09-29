# Database Performance Benchmark

This directory provides a reproducible benchmarking suite for analytical SQL queries across multiple embedded/columnar engines (DuckDB, SQLite and chDB/ClickHouse).

It is designed to measure query performance and resource usage (wall time, time-to-first-result, CPU, resident memory, and Python heap) and to enable fair comparisons between engines using the same datasets and query logic.

Key features:

- End-to-end workflow: ingest CSVs -> create engine-specific databases -> validate query equivalence -> run controlled benchmarks -> aggregate and analyse results.
- Multiple run modes: in-process (inproc), fresh child per run (cold starts), and persistent child (warm starts) to capture different operational scenarios.
- Lightweight monitoring: periodic sampling of CPU/RSS plus Linux true high-water RSS extraction, and Python heap peak via tracemalloc.
- Correctness-first: result comparison supports ordered and bag (multiset) equivalence, float tolerances, and JSON output for automation/CI.

Intended use:

1. Create engine-specific databases from raw CSVs.
2. Validate that different SQL implementations produce equivalent results (the SQL implementations must be provided by you).
3. Run benchmarks with configurable warmups, repeats, sampling interval, and threading.
4. Collect per-run metrics and produce aggregated summaries (means, percentiles) for reporting.

## File Structure

Before running benchmarks, ensure the following raw data files are present in the root directory (or adjust paths used by the creation scripts):

```ascii
raw_data/
├── acc
│   ├── acc_vs14.csv
│   └── acc_ab60.csv
├── grv
│   ├── grv_vs14.csv
│   └── grv_ab60.csv
└── ...
```

## Installation

Recommended (example):

```bash
pip install psutil pandas duckdb chdb matplotlib
```

> Both SQLite and DuckDB (also chdb) are database engines that are implemented as self-contained software libraries. In the Python ecosystem, they are distributed as wheel packages, so installing the corresponding Python package automatically provides the full database engine along with its Python bindings, without the need for any separate installation.

Required Python Packages (Could use Conda)

- `psutil` - System and process utilities
- `pandas` - Data manipulation and analysis
- `matplotlib` - Plotting graphs, charts, and other data visualizations
- `duckdb` - DuckDB database engine
- `chdb` - chDB database engine
- `sqlite3` - SQLite database engine (built-in)
- `tracemalloc` - Memory profiling (built-in)

## Usage

### Database Creation

Create databases from CSV data using `create_db.py`.

Parameters:

- `device_id` - user identifier
- `target_path` - Path for the output database file or directory
- `--engine` - Database engine: `duckdb` (default), `sqlite` and `chdb`
- `--post-sql` - SQL files to execute after database creation (e.g., for creating indexes)

Remember to specific root directories in `create_db.py`.

**Important**: Downgrade python version to 3.12 if you encounter issues with `new instance has no pybind11-registered base types`.

```bash
# Basic usage - create database
python create_db.py <device_id> <target_path> --engine {sqlite,duckdb,chdb}

# Get help
python create_db.py --help

# Examples 
# .duckdb or .sqlite is used to distinguish DuckDB from SQLite.
# will create DuckDB database in default.
python create_db.py vs14 ./test.duckdb
python create_db.py ab60 ./my_data.duckdb

# Demo

# Create a directory to store databases
mkdir -p db_vs14
# Create SQLite database
python create_db.py vs14 ./db_vs14/vs14_data.sqlite --engine sqlite

# Create DuckDB database
python create_db.py vs14 ./db_vs14/vs14_data.duckdb --engine duckdb

# Create chDB database
# chDB uses directory as DB, and need to specify the table
python create_db.py vs14 ./db_vs14/vs14_data_chdb --engine chdb

# Create with post-SQL (e.g., create indexes)
python create_db.py vs14 ./db_vs14/vs14_data.duckdb --engine duckdb --post-sql your_index.sql
```

### Prepare SQL

Prepare queries, and check their usablity. Execute them under path `benchmark`.

```bash
# DuckDB
python run_duckdb_sql.py ./db_vs14/vs14_data.duckdb ./queries/Q1/Q1_duckdb.sql > out_duckdb.csv

# SQLite
python run_sqlite_sql.py ./db_vs14/vs14_data.sqlite ./queries/Q1/Q1_sqlite.sql > out_sqlite.csv

# ClickHouse (chdb)
python run_chdb_sql.py ./db_vs14/vs14_data_chdb ./queries/Q1/Q1_clickhouse.sql > out_chdb.csv
```

### Validate SQL Correctness

Validate SQL correctness (that queries produce the same outcome) using `validate_sql_correctness.py`.

- `--mode`: Defines the **equivalence model** for result comparison:
  - **`ordered`**
    - Strict row-by-row comparison in order.
    - Any difference in order, row count, or values → FAIL.
    - Use when the query has a deterministic `ORDER BY` and you want to validate ordering.
  - **`bag`** (default)
    - Treats results as an **unordered multiset** of rows.
    - Checks only which rows appear and how many times, ignoring order.
    - Best for analytical queries without `ORDER BY`.
- `--float-tol`: Floating-point tolerance (default = `1e-9`). All floats are rounded before comparison to account for engine-specific floating-point differences.
  - Example: `--float-tol 1e-6` → round to 6 decimal places.
  - `--float-tol 0` → compare floats exactly.
- `--ignore-colnames`: Ignore column **names** and compare only column **counts**. Useful when engines generate different aliases but values are the same.
- `--output`: Controls output format:
  - **`human`** (default) → human-readable report (OK/FAIL + samples).
  - **`json`** → structured JSON summary (suitable for scripts or CI).
- `--show`: Maximum number of row difference **samples** to display in human mode (default = 5).
- `--json-file`: Write the full JSON summary to the specified file, in addition to console output.Useful for archiving results or machine parsing.

Sample usage:

```bash
python validate_sql_correctness.py \
  --case duckdb ./db_vs14/vs14_data.duckdb ./queries/Q1/Q1_duckdb.sql \
  --case sqlite ./db_vs14/vs14_data.sqlite ./queries/Q1/Q1_sqlite.sql \
  --case chdb   ./db_vs14/vs14_data_chdb  ./queries/Q1/Q1_clickhouse.sql \
  --mode bag --output human --show 5 --json-file q1_diff.json
```

This means:

- Run 3 queries (DuckDB, SQLite, ClickHouse).
- Compare results using **bag mode** (ignore order).
- Print a human-readable report, showing up to 5 sample differences.
- Save the complete JSON summary to `q1_diff.json`.

You have to make sure these 3 SQL files have the same outcome.

### Benchmarking

Use `benchmark.py` to measure performance and resource usage.

Command-line Arguments (Inputs):

| Argument             | Type / Values            | Description                                                                                 |
| -------------------- | ------------------------ | ------------------------------------------------------------------------------------------- |
| `--engine`           | `duckdb`,`sqlite`,`chdb` | Database engine to benchmark (default: `duckdb`).                                           |
| `--db-path`          | String (path)            | Path to database file/dir (`.duckdb` / `.sqlite` / chdb directory).                         |
| `--interval`         | Float (seconds)          | Sampling interval in seconds for CPU/RSS monitoring (default: 0.2).                         |
| `--query-file`       | String (path)            | Path to SQL file. If omitted, fallback to `queries/sample.sql` if present.                  |
| `--repeat`           | Integer                  | Number of measured runs (default: 10).                                                      |
| `--warmups`          | Integer                  | Number of warm-up runs not recorded (default: 0).                                           |
| `--child`            | Flag                     | Run each measured run in a separate child process.                                          |
| `--child-persistent` | Flag                     | Run all warmups + repeats against one persistent child/connection.                          |
| `--threads`          | Integer                  | DuckDB PRAGMA threads or chDB `max_threads` (0 = engine default); SQLite only use 1 thread. |
| `--out`              | String (path)            | If set, write all measured runs and summary as JSON to this path.                           |

Example commands:

```bash
python benchmark.py --engine duckdb --db-path ./db_vs14/vs14_data.duckdb \
  --query-file queries/Q1/Q1_duckdb.sql --threads 4 \
  --warmups 2 --repeat 10 --child-persistent --interval 0.2 \
  --out queries/Q1/duckdb_q1_persistent.json

python benchmark.py --engine sqlite --db-path ./db_vs14/vs14_data.sqlite \
  --query-file queries/Q1/Q1_sqlite.sql \
  --warmups 2 --repeat 10 --child-persistent --interval 0.2 \
  --out queries/Q1/sqlite_q1_persistent.json

python benchmark.py --engine chdb --db-path ./db_vs14/vs14_data_chdb \
  --query-file queries/Q1/Q1_clickhouse.sql --threads 4 \
  --warmups 2 --repeat 10 --child-persistent --interval 0.2 \
  --out queries/Q1/clickhouse_q1_persistent.json
```

Each run produces a JSON object with these keys:

| Field                     | Meaning                                                                                  |
| ------------------------- | ---------------------------------------------------------------------------------------- |
| `retval`                  | Return value from the last executed statement (row count for SELECT, "OK" for others).   |
| `wall_time_seconds`       | Wall-clock time for the run (seconds, measured by parent).                               |
| `ttfr_seconds`            | Time to first result (seconds).                                                          |
| `rows_returned`           | Number of rows returned by the query.                                                    |
| `statements_executed`     | Total number of SQL statements executed.                                                 |
| `select_statements`       | Number of SELECT statements executed.                                                    |
| `peak_rss_bytes_sampled`  | Peak Resident Set Size (RSS) sampled periodically.                                       |
| `peak_rss_bytes_true`     | True high-water RSS (from `/proc/[pid]/status` on Linux or OS API; fallback to sampled). |
| `cpu_avg_percent`         | Average CPU utilization during run (%).                                                  |
| `samples`                 | Number of monitoring samples collected.                                                  |
| `python_heap_peak_bytes`  | Peak Python heap usage (from `tracemalloc`; excludes DB C-layer memory).                 |
| `child_wall_time_seconds` | (Child modes only) Wall-clock time observed inside child process.                        |
| `mode`                    | Run mode: `"inproc"`, `"child"`, `"child-persistent"`.                                   |

Summary Output Fields (summary object):

Aggregated across all measured runs (warmups excluded):

| Field                      | Meaning                                              |
| -------------------------- | ---------------------------------------------------- |
| `engine`                   | Database engine used.                                |
| `mode`                     | Run mode (`inproc`, `child`, or `child-persistent`). |
| `db_path`                  | Path to the database file/dir.                       |
| `query_file`               | Path to SQL query file executed.                     |
| `repeat`                   | Number of measured runs.                             |
| `warmups`                  | Number of warm-up runs excluded.                     |
| `threads`                  | Number of threads (DuckDB/chDB).                     |
| `mean_wall_time_seconds`   | Mean wall time across measured runs.                 |
| `mean_ttfr_seconds`        | Mean TTFR across measured runs.                      |
| `mean_peak_rss_bytes_true` | Mean peak true RSS (bytes).                          |
| `mean_cpu_avg_percent`     | Mean CPU average (%).                                |
| `mean_rows_returned`       | Mean number of rows returned.                        |
| `p50_wall_time_seconds`    | Median (P50) wall time.                              |
| `p95_wall_time_seconds`    | 95th percentile wall time.                           |
| `p99_wall_time_seconds`    | 99th percentile wall time.                           |
| `p50_ttfr_seconds`         | Median (P50) TTFR.                                   |
| `p95_ttfr_seconds`         | 95th percentile TTFR.                                |
| `p99_ttfr_seconds`         | 99th percentile TTFR.                                |

---

Not always – `--child-persistent` is not the best choice in every case.

- Use **`--child`** when you want *cold-start behavior*: each run creates a new process and connection, so no connection-level caches carry over.
- Use **`--child-persistent`** for *warm scenarios*: warmups and repeats all run in one child process, so session-level caches (SQLite page cache, DuckDB session state) can persist.
- Use **`--inproc`** for quick debugging with minimal overhead. For serious experiments, it’s often useful to report both cold (`--child`) and warm (`--child-persistent`) results.

As for `--interval`: it controls how often CPU/RSS are sampled. Too large → you may miss spikes; too small → extra overhead. A good rule of thumb is to aim for ~20–100 samples per run.

- Short queries (<0.2s): 0.02–0.05s
- Medium (0.2–2s): 0.05–0.1s
- Long (2–30s): 0.1–0.5s
- Very long (>30s): 0.5–1s

Practical approach: run once with a rough interval, check the wall time, then set `interval ≈ wall_time / 50` for your main repeats. This way you balance fidelity and monitoring cost.

### Complete Workflow Example

Step 1: Create databases from device vs14 data

```bash
mkdir -p db_vs14
python create_db.py vs14 ./db_vs14/vs14_data.duckdb --engine duckdb
python create_db.py vs14 ./db_vs14/vs14_data.sqlite --engine sqlite
python create_db.py vs14 ./db_vs14/vs14_data_chdb --engine chdb
```

Step 2: Prepare SQL files and make sure they have the same outcome

```bash
python run_duckdb_sql.py ./db_vs14/vs14_data.duckdb queries/Q1/Q1_duckdb.sql > out_duckdb.csv
python run_sqlite_sql.py ./db_vs14/vs14_data.sqlite queries/Q1/Q1_sqlite.sql > out_sqlite.csv
python run_chdb_sql.py ./db_vs14/vs14_data_chdb queries/Q1/Q1_clickhouse.sql > out_chdb.csv

python validate_sql_correctness.py \
  --case duckdb ./db_vs14/vs14_data.duckdb queries/Q1/Q1_duckdb.sql \
  --case sqlite ./db_vs14/vs14_data.sqlite queries/Q1/Q1_sqlite.sql \
  --case chdb   ./db_vs14/vs14_data_chdb  queries/Q1/Q1_clickhouse.sql \
  --mode bag --output human --show 5 --json-file q1_diff.json
```

Step 3: Prepare the `datasets` and `query_groups` in `config.yaml`.

Step 4: Benchmark databases

```bash
# Orchestrate benchmark experiments across multiple database engines, datasets, and queries.
python run_experiments.py

# Validate or analyse collected results.
python analyze_results.py
```
