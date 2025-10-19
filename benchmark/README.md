# Database Benchmark System

A comprehensive benchmarking framework for comparing database performance across multiple engines (DuckDB, SQLite, chDB) with automated monitoring, profiling, and visualization.

## Overview

This benchmark system provides:

- **Automated execution**: Run experiments across multiple databases and queries
- **Resource monitoring**: Track CPU usage, memory consumption, and execution time
- **Statistical analysis**: Aggregate results with percentiles (P50, P95, P99)
- **Visualization**: Generate comparison charts automatically
- **Configuration-driven**: Simple YAML configuration

### Key Features

1. **Two-stage execution model**:
   - Stage 1: Automatic sampling interval calculation (pilot runs)
   - Stage 2: Full benchmark with optimized monitoring
2. **Comprehensive metrics**:
   - Execution time statistics
   - CPU utilization
   - Memory usage
   - Query output validation (row counts)
3. **Clear logging**: Structured logs with stage markers and progress indicators
4. **Easy visualization**: Automated chart generation for performance comparison
5. **SQL correctness validation**: Verify query equivalence across different database queries

## Architecture

```ASCII
benchmark/
├── config.yaml                  # Main configuration file (EDIT THIS)
├── config_dev.yaml              # single experiment
├── run_experiments.py           # Execute benchmarks
├── analyze_results.py           # Generate visualizations
├── create_db.py                 # Create databases from CSV data
├── validate_sql_correctness.py  # Validate SQL correctness across queries
│
├── config/                # Configuration loading
│   ├── config_loader.py
│   ├── benchmark_config.py
│   ├── dataset.py
│   ├── execution_unit.py
│   └── query_group.py
│   cli/
│   └── cli.py
│
└── service/
    ├── runner/            # Database execution
    │   ├── duckdb_runner.py
    │   ├── chdb_runner.py
    │   └── sqlite_runner.py
    ├── monitor/           # Resource monitoring
    │   ├── process_monitor.py
    │   ├── process_monitor_result.py
    │   └── process_snapshot.py
    ├── task_executor/     # Experiment orchestration
    │   ├── task_executor.py
    │   └── task_execute_result.py
    └── profile_parser/    # Log parsing
        ├── query_metric.py
        ├── chdb_log_parser.py
        ├── duckdb_log_parser.py
        └── sqlite_log_parser.py
```

## Runtime Environment

Platform: `linux-x86_64`

- **SQLite:** >=3.43.2
- **DuckDB:** >=1.4.1
- **chDB**: >=3.6.0

## Quick Start

### Install Database engine

SQLite:

⚠️ Important: Compile SQLite with Profiling Support

Before running benchmarks with SQLite, you **must** compile SQLite with the `SQLITE_ENABLE_STMT_SCANSTATUS` flag enabled to support query profiling and performance metrics.

📖 **See [COMPILE_SQLITE](COMPILE_SQLITE.md) for detailed compilation instructions.**

Without this flag, SQLite profiling features will not work, and benchmark results will be incomplete.

chDB:

chDB even doesn't have CLI. So we need to develop a simple one ourselves.

📖 **See [chdb_cli - ChDB Command Line Tool](chdb_cli/README.md) for detailed compilation instructions.**

DuckDB:

```bash
# <https://duckdb.org/install/?platform=linux&environment=cli&architecture=x86_64>
curl https://install.duckdb.org | sh

# CLI will tell you how to append the following line to your shell profile
# then
source ~/.profile
```

### 1. Install Dependencies

```bash
pip install psutil pandas duckdb matplotlib pyyaml
```

### 2. Create Databases

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

### 3. Configure Experiments

Edit `config.yaml` to define the base query settings, then edit `config_<exp>.yaml` for single experiment:

Example `config.yaml`:

```yaml
engines: [duckdb, sqlite, chdb]
repeat_pilot: 3
std_repeat: 5

engine_paths:
  duckdb: duckdb
  sqlite: sqlite3
  chdb: ./chdb_cli/chdb_cli

datasets:
  - name: vs14
    duckdb_db: ./db_vs14/vs14_data.duckdb
    sqlite_db: ./db_vs14/vs14_data.sqlite
    chdb_db_dir: ./db_vs14/vs14_data_chdb

query_groups:
  - id: sample-Q1
    duckdb_sql: ./queries/Q1/Q1_duckdb.sql
    sqlite_sql: ./queries/Q1/Q1_sqlite.sql
    chdb_sql: ./queries/Q1/Q1_clickhouse.sql
```

Example `config_dev.yaml`:

```yaml
validate_pairs:
  - [ sample-Q1, duckdb ]
  - [ sample-Q1, sqlite ]
  - [ sample-Q1, chdb ]

execute_pairs:
  - [ sample-Q1, duckdb ]
  - [ sample-Q1, sqlite ]
  - [ sample-Q1, chdb ]

compare_pairs:
  - [ sample-Q1, duckdb ]
  - [ sample-Q1, sqlite ]
  - [ sample-Q1, chdb ]
```

### 4. Validate SQL Correctness (Recommended)

Before running benchmarks, verify that queries produce identical results across different database queries.

**Configuration:**

First, configure validation pairs in `config_<exp>.yaml`:

```yaml
# Step 1: Define your queries in query_groups
query_groups:
  - id: sample-Q1
    duckdb_sql: ./queries/Q1/Q1_duckdb.sql
    sqlite_sql: ./queries/Q1/Q1_sqlite.sql
    chdb_sql: ./queries/Q1/Q1_clickhouse.sql

# Step 2: Specify which query results to validate
validate_pairs:
  - [ sample-Q1, duckdb ]
  - [ sample-Q1, sqlite ]
  - [ sample-Q1, chdb ]
```

The validation script will:

1. Execute each query specified in `validate_pairs`
2. Compare results pairwise: If you have $n$ queries in `validate_pairs`, it performs $C_n^2 = n \times (n-1)/2$ comparisons
   - Example: 2 queries → 1 comparison, 3 queries → 3 comparisons, 4 queries → 6 comparisons
3. Report any differences found

> **⚠️ Important**: You must first configure queries in `query_groups` before adding them to `validate_pairs`.

**Run validation:**

```bash
# Use environment override to load config_<exp>.yaml alongside config.yaml
python validate_sql_correctness.py --env dev
```

**Output example:**

```bash
============================================================
  SQL CORRECTNESS VALIDATION
============================================================

📋 Configuration:
   • Total experiments: 4
   • Validation pairs: 3
   • Numeric tolerance: rtol=1e-05, atol=1e-08
   • Timestamp auto-conversion: enabled

🔧 Running validations...
   [1] trend_Q1_duckdb... ✓
   [2] trend_Q1_sqlite... ✓
   [3] trend_Q2_duckdb... ✓

============================================================
  RESULTS COMPARISON
============================================================

🔍 trend_Q1_duckdb ↔ trend_Q1_sqlite
  ✅ Results are identical (1000 rows)

🔍 trend_Q2_duckdb ↔ trend_Q2_sqlite
  ❌ Row 5: 1 column(s) differ
     Column 1: '42.5' ≠ '43.2'
  ⚠️  Found 1 row(s) with differences

============================================================
  SUMMARY
============================================================
   • Total comparisons: 2
   • Identical: 1
   • Different: 1

   ⚠️  1 comparison(s) failed!
============================================================
```

**Features:**

- **Numeric precision handling**: Small floating-point differences (within tolerance) are automatically ignored to avoid false positives from precision issues
- **Timestamp auto-conversion**: Automatically handles different time formats (Unix timestamps in seconds/milliseconds, ISO 8601, common datetime formats)
  - Example: `'2021-03-04 07:42:00'` and `'1614843720000'` are recognized as the same time
- **Column-by-column comparison**: Precisely identifies which rows and columns differ
- **Intelligent type conversion**: Automatically compares numeric values even when stored as strings
- **Configurable tolerance**: Adjust `NUMERIC_RTOL` and `NUMERIC_ATOL` in `validate_sql_correctness.py` if needed (default: `rtol=1e-5`, `atol=1e-8`)

**💡 Tip - SQL Syntax Checking:**

You can also use this tool to quickly check if your SQL queries run without errors:

- Run the validation script
- If there are syntax errors, the script will abort and show the error details
- Ignore comparison warnings if you're only checking syntax

Example error output when SQL has syntax errors:

```bash
🔧 Running validations...
   [1] Q2_duckdb... ❌

============================================================
  ERROR: Validation failed for Q2_duckdb
============================================================
   Return code: 1

   Error output:
   Error: near line 5: syntax error

============================================================
   Validation aborted due to execution failure.
============================================================
```

This is useful for rapid SQL debugging before running full benchmarks.

### 5. Run Benchmarks

```bashs
# Use environment-specific overrides (loads `config_dev.yaml` in addition to `config.yaml`)
python run_experiments.py --env dev
```

### 6. Generate Visualizations

```bash
# With environment override (e.g., reads config_dev.yaml on top of config.yaml)
python analyze_results.py --env dev
```

## Configuration

All experiments are configured through `config.yaml`, with optional environment-specific overrides loaded via the `--env` flag.

### Environment Overrides

- Base settings live in `config.yaml`; treat this as your global defaults.
- Add partial overrides in files named `config_<exp>.yaml` (e.g., `config_dev.yaml`).
- Run `run_experiments.py`, `analyze_results.py`, or `validate_sql_correctness.py` with `--env <name>` to merge the override file onto the base config.
- The loader always falls back to `config.yaml`, so missing keys in the override keep their default values.

### Core Parameters

| Parameter      | Description                                     | Default                 |
|----------------|-------------------------------------------------|-------------------------|
| `engines`      | Database engines to benchmark                   | `[duckdb, sqlite，chdb]` |
| `repeat_pilot` | Pilot runs for interval calculation (Stage 1/2) | `3`                     |
| `sample_count` | Target monitoring samples per query             | `10`                    |
| `std_repeat`   | Benchmark iterations (Stage 2/2)                | `5`                     |
| `output_cwd`   | Results output directory                        | `./results`             |

### Execution Model

Stage 1/2: Calculate Sampling Interval

- Runs `repeat_pilot` times (default: 3)
- Uses fixed interval (10 seconds)
- Calculates optimal monitoring interval: `interval = avg_time / sample_count`

Stage 2/2: Run Benchmark

- Runs `std_repeat` times (default: 5)
- Uses calculated interval from Stage 1
- Collects full statistics (avg, P50, P95, P99)

### Example Configuration

See detailed comments in [`config.yaml`](config.yaml) for more information.

### Validation Configuration

The `validate_pairs` parameter defines which experiments to run for SQL correctness validation:

- **Purpose**: Verify that queries produce identical results across different database queries
- **Format**: `[ query_group_id, engine ]` pairs
- **Used by**: `validate_sql_correctness.py`
- **Behavior**: The script executes specified queries and compares outputs line-by-line to ensure query equivalence.

## Complete Workflow Example

### Step 1: Prepare Data

```bash
# Ensure raw CSV files are in place
ls raw_data/acc/acc_vs14.csv
ls raw_data/hrm/hrm_vs14.csv
# ... etc

# Create in benchmark/ directory
mkdir -p db_vs14
```

### Step 2: Create Databases

```bash
# DuckDB
python create_db.py vs14 ./db_vs14/vs14_data.duckdb --engine duckdb

# SQLite
python create_db.py vs14 ./db_vs14/vs14_data.sqlite --engine sqlite

# chDB
python create_db.py vs14 ./db_vs14/vs14_data_chdb --engine chdb

# Optional: Create with post-SQL (indexes, etc.)
python create_db.py vs14 ./db_vs14/vs14_data.duckdb \
  --engine duckdb \
  --post-sql ./queries/create_indexes.sql
```

### Step 3: Configure Experiments

Edit `config.yaml` and `config_<exp>.yaml`:

1. Add your datasets under `datasets:`
2. Add your queries under `query_groups:`
3. Define comparison pairs under `compare_pairs:`
4. Define validation pairs under `validate_pairs:`

### Step 4: Validate SQL Correctness

Verify that queries produce identical results across different queries.

Configure which experiments to validate in `config_<exp>.yaml` under `validate_pairs:`.

```bash
python validate_sql_correctness.py --env dev
```

This step is useful to:

- Ensure queries are logically equivalent across different SQL dialects
- Detect subtle differences in query results between engines

### Step 5: Run Benchmarks

```bash
python run_experiments.py --env dev
```

> **ℹ️ Note**: When running benchmarks or validations, the program automatically creates temporary SQL files (with `_profiling_tmp.sql` or `_validate_tmp.sql` suffix) that include necessary configurations. Your original SQL files remain unchanged. These temporary files are already configured in `.gitignore`.

**Expected output:**

```bash
[INFO] ============================================================
[INFO] Starting Benchmark Experiments
[INFO] ============================================================
[INFO] Loaded 4 experiments from config
[INFO] 
[INFO] ------------------------------------------------------------
[INFO] Experiment 1/4: Q1_aggregation (DUCKDB)
[INFO] ------------------------------------------------------------
[INFO] Stage 1/2: Calculating sampling interval (pivot runs: 3)
[INFO]   Run 1/3: Executing query...
[INFO]   Run 1/3: Time=0.85s, CPU=92.5%, Memory=128.4MB, Rows=25051
[INFO]   Run 2/3: Executing query...
[INFO]   Run 2/3: Time=0.82s, CPU=91.8%, Memory=127.9MB, Rows=25051
[INFO]   Run 3/3: Executing query...
[INFO]   Run 3/3: Time=0.83s, CPU=92.1%, Memory=128.1MB, Rows=25051
[INFO]   Aggregating 3 run(s)...
[INFO]   → Avg=0.833s, P50=0.830s, P95=0.847s
[INFO] ✓ Stage 1/2 completed: interval=0.042s (avg time=0.833s)
[INFO] Stage 2/2: Running benchmark (5 iterations, interval=0.042s)
[INFO]   Run 1/5: Executing query...
[INFO]   Run 1/5: Time=0.84s, CPU=92.3%, Memory=128.2MB, Rows=25051
...
[INFO] ✓ Stage 2/2 completed: Time(avg)=0.840s, CPU(avg)=92.1%, Memory(peak)=128.4MB
[INFO] ✓ Experiment 1/4 completed
...
[INFO] ============================================================
[INFO] Exporting Results
[INFO] ============================================================
[INFO] ✓ Results exported to: /path/to/results/summary.json
[INFO] 
[INFO] All experiments completed successfully!
```

### Step 6: Generate Visualizations

```bash
python analyze_results.py --env dev
```

Charts will be saved to `results/visual/`:

- `execution_time_comparison.png`
- `memory_usage_comparison.png`
- `cpu_usage_comparison.png`
- `throughput_comparison.png`
- Individual comparison charts for each pair

## Output and Results

### `summary.json` Structure

```json
{
  "Q1_aggregation": {
    "duckdb": {
      "cpu_peek_percent": {
        "min": 89.5,
        "max": 95.2,
        "p50": 92.1,
        "p95": 94.8,
        "p99": 95.1,
        "avg": 92.3
      },
      "execution_time": {
        "min": 0.82,
        "max": 0.87,
        "p50": 0.84,
        "p95": 0.86,
        "p99": 0.87,
        "avg": 0.843
      },
      "peak_memory_bytes": {
        "min": 127000000,
        "max": 129000000,
        "p50": 128000000,
        "p95": 128900000,
        "p99": 128950000,
        "avg": 128100000
      },
      "output_rows": 25051
    },
    "sqlite": {
      ...
    }
  }
}
```

### Visualization Output

Generated charts in `results/visual/`:

1. **Execution Time Comparison**: Bar chart comparing query execution times
2. **Memory Usage Comparison**: Peak memory consumption across engines
3. **CPU Usage Comparison**: Average CPU utilization
4. **Throughput Comparison**: Rows processed per second
5. **Individual Pair Charts**: Detailed 2x3 comparison grids

All charts include:

- Color-coded bars (blue=DuckDB, orange=SQLite, green=chDB)
- Clear axis labels and titles
- Professional styling

## Advanced Topics

### Custom Engine Paths

If using custom-builds or specific versions:

```yaml
engine_paths:
  duckdb: /usr/local/bin/duckdb
  sqlite: /Users/me/custom-sqlite/sqlite3
```

Or edit the `PATH` permanently in `~/.profile` / temporarily in the current shell.

### Adjusting Monitoring Granularity

For faster queries, increase `sample_count` for finer monitoring:

```yaml
sample_count: 50  # More samples = finer CPU/memory tracking
```

For slower queries, decrease to reduce overhead:

```yaml
sample_count: 5   # Fewer samples = less monitoring overhead
```

### Multiple Datasets

Benchmark the same query across different datasets:

```yaml
datasets:
  - name: small_dataset
    duckdb_db: ./db_small/data.duckdb
    sqlite_db: ./db_small/data.sqlite

  - name: large_dataset
    duckdb_db: ./db_large/data.duckdb
    sqlite_db: ./db_large/data.sqlite

query_groups:
  - id: Q1_small
    duckdb_sql: ./queries/Q1_duckdb.sql
    sqlite_sql: ./queries/Q1_sqlite.sql

  - id: Q1_large
    duckdb_sql: ./queries/Q1_duckdb.sql
    sqlite_sql: ./queries/Q1_sqlite.sql
```

### Logging Configuration

The system uses a unified logging framework (`util/log_config.py`).

To enable debug logging for troubleshooting:

```python
from util.log_config import setup_logger
import logging

logger = setup_logger(__name__, level=logging.DEBUG)
```
