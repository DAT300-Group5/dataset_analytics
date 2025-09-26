# Database Performance Benchmark

This directory contains tools for benchmarking and comparing the performance of different database engines (DuckDB vs SQLite) for analytical queries.

## Overview

The benchmark suite provides:

- **Performance measurement tools** for CPU and memory usage
- **Query modules** for DuckDB and SQLite
- **Database creation utilities** for test data
- **Comparative analysis** between different database engines

## File Structure

Before running benchmarks, ensure the following raw data files are present in the directory:

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

Remember to specific root directories in `create_db.py`.

## Installation

### Prerequisites

```bash
pip install psutil pandas duckdb
```

> Both SQLite and DuckDB are database engines that are implemented as self-contained software libraries. In the Python ecosystem, they are distributed as wheel packages, so installing the corresponding Python package automatically provides the full database engine along with its Python bindings, without the need for any separate installation.

Required Python Packages (Could use Conda)

- `psutil` - System and process utilities
- `pandas` - Data manipulation and analysis
- `duckdb` - DuckDB database engine
- `sqlite3` - SQLite database engine (built-in)
- `tracemalloc` - Memory profiling (built-in)

## Usage

### Database Creation

Create databases from CSV data using `create_db.py`.

Parameters:

- `device_id` - user identifier
- `target_path` - Path for the output database file
- `--engine` - Database engine: `duckdb` (default) or `sqlite`

```bash
# Basic usage - create database
python create_db.py <device_id> <target_path> --engine {sqlite,duckdb}

# Get help
python create_db.py --help

# Examples 
# .duckdb or .sqlite is used to distinguish DuckDB from SQLite.
# will create DuckDB database in default.
python create_db.py vs14 ./test.duckdb
python create_db.py ab60 ./my_data.duckdb

# Create SQLite database
python create_db.py vs14 ./test.sqlite --engine sqlite
```

### Benchmarking

Command-line Arguments (Inputs):

| Argument                  | Type / Values                                    | Description                                                                                                                                                         |
| ------------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--engine`                | `duckdb` or `sqlite` (default: `duckdb`)         | Select the backend database engine. Always record the exact version in experiments.                                                                                 |
| `--db-path PATH`          | String (required)                                | Path to the database file (`.duckdb` or `.sqlite`).                                                                                                                 |
| `--query-file PATH`       | String (optional, default: `queries/sample.sql`) | Path to an SQL text file. Multiple statements allowed (semicolon-separated). TTFR is measured on the **first SELECT**; rows are counted for the **last SELECT**.    |
| `--interval FLOAT`        | Seconds (default: 0.2)                           | Sampling interval for CPU% and RSS monitoring. Short queries require smaller values (e.g., 0.05s) to avoid missing spikes.                                          |
| `--repeat INT`            | Default: 10                                      | Number of measured runs to record. Percentiles (P95/P99) are meaningful only if `repeat` is reasonably large (≥5).                                                  |
| `--warmups INT`           | Default: 0                                       | Number of warm-up runs, executed but **not recorded**. Useful for “warm cache” scenarios.                                                                           |
| `--child`                 | Flag                                             | Run each measured run in a fresh child process (new DB connection every run). Good isolation, but no connection-level caching.                                      |
| `--child-persistent`      | Flag                                             | Run all warmups + repeats inside **one persistent child process/connection**. Allows connection/session-level caches to persist. Mutually exclusive with `--child`. |
| `--threads INT`           | DuckDB only, default: 0                          | Sets `PRAGMA threads=<k>`. `0` means leave DuckDB’s default. Fix this for reproducibility.                                                                          |
| `--sqlite-journal MODE`   | SQLite only                                      | Sets `PRAGMA journal_mode` (e.g., WAL, OFF, DELETE).                                                                                                                |
| `--sqlite-sync MODE`      | SQLite only                                      | Sets `PRAGMA synchronous` (OFF, NORMAL, FULL, EXTRA).                                                                                                               |
| `--sqlite-cache-size INT` | SQLite only                                      | Sets `PRAGMA cache_size`. Positive = number of pages; negative = size in KiB.                                                                                       |
| `--out PATH`              | String                                           | If given, write per-run results (`runs`) and aggregated statistics (`summary`) to a JSON file.                                                                      |

Each run produces a JSON object with these keys:

| Field                       | Meaning                                                                                                                                               |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `retval`                    | Return value or status from `run_query_with_ttfr`.                                                                                                    |
| `wall_time_seconds`         | Wall-clock time observed by the parent (start → end of monitoring). Includes monitoring overhead.                                                     |
| `child_wall_time_seconds`   | (Child or persistent modes only) Pure workload wall time measured inside the child. Excludes parent overhead.                                         |
| `ttfr_seconds`              | Time-to-first-result for the **first SELECT**. From `cursor.execute()` to fetching the first batch (`fetchmany(10000)`). Models “first page latency.” |
| `rows_returned`             | Total number of rows returned by the **last SELECT**. Helps explain cases where TTFR is small but total wall time is large.                           |
| `statements_executed`       | Total number of SQL statements executed from the file.                                                                                                |
| `select_statements`         | Number of `SELECT` statements executed.                                                                                                               |
| `peak_rss_bytes_sampled`    | Peak Resident Set Size observed from periodic sampling. May miss short spikes.                                                                        |
| `peak_rss_bytes_true`       | True high-water mark of RSS: VmHWM on Linux, peak working set on Windows. More reliable than sampled.                                                 |
| `cpu_avg_percent`           | Average process CPU% over all samples. Can exceed 100% on multi-core.                                                                                 |
| `samples`                   | Number of samples collected during monitoring. ≈ 1 + floor(run_time / interval).                                                                      |
| `python_heap_peak_bytes`    | Peak Python heap (from `tracemalloc`), Python objects only. Excludes DuckDB/SQLite C allocations and OS page cache.                                   |
| `db_memory_used_bytes`      | (SQLite only) PRAGMA `memory_used`. Memory currently allocated by SQLite.                                                                             |
| `db_memory_highwater_bytes` | (SQLite only) PRAGMA `memory_highwater`. Peak memory allocated by SQLite during the session.                                                          |
| `db_memory_usage_bytes`     | (DuckDB only, best effort) PRAGMA `memory_usage`. Aggregated into one number if possible.                                                             |
| `mode`                      | Run mode: `"inproc"`, `"child"`, or `"child-persistent"`.                                                                                             |

Summary Output Fields (summary object):

Aggregated across all measured runs (warmups excluded):

| Field                            | Meaning                                         |
| -------------------------------- | ----------------------------------------------- |
| `engine`                         | `"duckdb"` or `"sqlite"`.                       |
| `mode`                           | `"inproc"`, `"child"`, or `"child-persistent"`. |
| `db_path`                        | Path to the database file.                      |
| `query_file`                     | Path to the SQL file used.                      |
| `repeat`                         | Number of measured runs.                        |
| `warmups`                        | Number of warmup runs.                          |
| `threads`                        | DuckDB threads setting.                         |
| `sqlite_pragmas`                 | Dictionary of PRAGMAs applied.                  |
| `mean_wall_time_seconds`         | Average wall time across runs.                  |
| `mean_ttfr_seconds`              | Average TTFR across runs.                       |
| `mean_peak_rss_bytes_true`       | Average of true RSS peaks.                      |
| `mean_cpu_avg_percent`           | Average CPU% across runs.                       |
| `mean_rows_returned`             | Average rows returned.                          |
| `p50_wall_time_seconds`          | Median wall time (50th percentile).             |
| `p95_wall_time_seconds`          | 95th percentile wall time.                      |
| `p99_wall_time_seconds`          | 99th percentile wall time.                      |
| `p50_ttfr_seconds`               | Median TTFR.                                    |
| `p95_ttfr_seconds`               | 95th percentile TTFR.                           |
| `p99_ttfr_seconds`               | 99th percentile TTFR.                           |
| `mean_db_memory_used_bytes`      | SQLite only. Mean `memory_used`.                |
| `mean_db_memory_highwater_bytes` | SQLite only. Mean `memory_highwater`.           |
| `mean_db_memory_usage_bytes`     | DuckDB only. Mean aggregated `memory_usage`.    |

Example Commands:

DuckDB: warm scenario, 5 repeats, persistent child

```shell
python benchmark.py --engine duckdb --db-path ./data.duckdb \
  --query-file queries/Q1.sql --threads 4 \
  --warmups 1 --repeat 5 --child-persistent --interval 0.1 \
  --out duckdb_q1_persistent.json
```

SQLite: WAL mode, warm scenario, persistent child

```shell
python benchmark.py --engine sqlite --db-path ./foo.sqlite \
  --query-file queries/Q2.sql \
  --sqlite-journal WAL --sqlite-sync NORMAL --sqlite-cache-size 200000 \
  --warmups 2 --repeat 7 --child-persistent --interval 0.2 \
  --out sqlite_q2_persistent.json
```

```shell
DuckDB: cold runs, one-shot child processes
python benchmark.py --engine duckdb --db-path ./data.duckdb \
  --query-file queries/Q1.sql \
  --repeat 10 --child --interval 0.1 \
  --out duckdb_q1_cold.json
```

SQLite: simple in-process runs

```shell
python benchmark.py --engine sqlite --db-path ./foo.sqlite \
  --query-file queries/Q3.sql \
  --repeat 5 --interval 0.2 \
  --out sqlite_q3_inproc.json
```

### Complete Workflow Example

Here's a complete example of creating databases and benchmarking them:

```bash
# Step 1: Create databases from device vs14 data
python create_db.py vs14 ./vs14_data.duckdb --engine duckdb
python create_db.py vs14 ./vs14_data.sqlite --engine sqlite

# Step 2: Benchmark both databases
python benchmark.py --engine duckdb --db-path ./vs14_data.duckdb \
  --query-file queries/Q1/Q1_duckdb.sql --threads 4 \
  --warmups 2 --repeat 1 --child-persistent --interval 0.2 \
  --out queries/Q1/duckdb_q1_persistent.json

python benchmark.py --engine sqlite --db-path ./vs14_data.sqlite \
  --query-file queries/Q1/Q1_sqlite.sql \
  --warmups 2 --repeat 1 --child-persistent --interval 0.2 \
  --out queries/Q1/sqlite_q1_persistent.json
```

### Q & A

Not always – `--child-persistent` is not the best choice in every case.

- Use **`--child`** when you want *cold-start behavior*: each run creates a new process and connection, so no connection-level caches carry over.
- Use **`--child-persistent`** for *warm scenarios*: warmups and repeats all run in one child process, so session-level caches (SQLite page cache, DuckDB session state) can persist.
- Use **inproc** for quick debugging with minimal overhead. For serious experiments, it’s often useful to report both cold (`--child`) and warm (`--child-persistent`) results.

As for `--interval`: it controls how often CPU/RSS are sampled. Too large → you may miss spikes; too small → extra overhead. A good rule of thumb is to aim for ~20–100 samples per run.

- Short queries (<0.2s): 0.02–0.05s
- Medium (0.2–2s): 0.05–0.1s
- Long (2–30s): 0.1–0.5s
- Very long (>30s): 0.5–1s

Practical approach: run once with a rough interval, check the wall time, then set `interval ≈ wall_time / 50` for your main repeats. This way you balance fidelity and monitoring cost.
