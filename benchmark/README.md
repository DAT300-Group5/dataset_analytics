# Database Benchmark System

A comprehensive benchmarking framework for comparing database performance across multiple engines (DuckDB, SQLite, chDB) with automated monitoring, profiling, and visualization.

> **📌 Note**: To use SQLite benchmarking features, you must compile SQLite with profiling support. See [COMPILE_SQLITE.md](./COMPILE_SQLITE.md) for instructions.
>
> **🚧 TODO**: chDB benchmark support is not yet fully implemented. Currently only DuckDB and SQLite are supported for benchmarking.

## Overview

This benchmark system provides:

- **Automated execution**: Run experiments across multiple databases and queries
- **Resource monitoring**: Track CPU usage, memory consumption, and execution time
- **Statistical analysis**: Aggregate results with percentiles (P50, P95, P99)
- **Visualization**: Generate comparison charts automatically
- **Configuration-driven**: Simple YAML configuration, no complex command-line arguments

### Key Features

✅ **Two-stage execution model**:

- Stage 1: Automatic sampling interval calculation (pilot runs)
- Stage 2: Full benchmark with optimized monitoring

✅ **Comprehensive metrics**:

- Execution time statistics
- CPU utilization
- Memory usage
- Query output validation (row counts)

✅ **Clear logging**: Structured logs with stage markers and progress indicators

✅ **Easy visualization**: Automated chart generation for performance comparison

## Architecture

```ASCII
benchmark/
├── config.yaml              # Main configuration file (EDIT THIS)
├── run_experiments.py       # Execute benchmarks
├── analyze_results.py       # Generate visualizations
├── create_db.py            # Create databases from CSV data
├── validate_sql_correctness.py  # Validate query equivalence
│
├── config/                 # Configuration loading
│   ├── config_loader.py
│   ├── benchmark_config.py
│   ├── dataset.py
│   └── query_group.py
│
├── service/
│   ├── runner/            # Database execution
│   │   ├── duckdb_runner.py
│   │   └── sqlite_runner.py
│   ├── monitor/           # Resource monitoring
│   │   └── process_monitor.py
│   ├── task_executor/     # Experiment orchestration
│   │   └── task_executor.py
│   └── proflie_parser/    # Log parsing
│       ├── duckdb_log_parser.py
│       └── sqlite_log_parser.py
│
├── queries/               # SQL query files
│   └── ...
│
├── db_vs14/              # Database files
│   ├── vs14_data.duckdb
│   ├── vs14_data.sqlite
│   └── vs14_data_chdb/
│
└── results/              # Output directory
    ├── summary.json      # Aggregated results
    └── visual/          # Generated charts
```

## Quick Start

### ⚠️ Important: Compile SQLite with Profiling Support

Before running benchmarks with SQLite, you **must** compile SQLite with the `SQLITE_ENABLE_STMT_SCANSTATUS` flag enabled to support query profiling and performance metrics.

📖 **See [COMPILE_SQLITE.md](./COMPILE_SQLITE.md) for detailed compilation instructions.**

Without this flag, SQLite profiling features will not work, and benchmark results will be incomplete.

### 1. Install Dependencies

```bash
pip install psutil pandas duckdb matplotlib pyyaml
```

```bash
sudo snap install duckdb
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

Edit `config.yaml` to define your experiments:

```yaml
# 🚧 Note: Only 'duckdb' and 'sqlite' are currently supported
# chDB support is TODO
engines: [duckdb, sqlite]
repeat_pilot: 3
std_repeat: 5

engine_paths:
  duckdb: duckdb
  sqlite: ../sqlite/bin/sqlite3

datasets:
  - name: vs14
    duckdb_db: ./db_vs14/vs14_data.duckdb
    sqlite_db: ./db_vs14/vs14_data.sqlite

query_groups:
  - id: Q1
    duckdb_sql: ./queries/Q1/Q1_duckdb.sql
    sqlite_sql: ./queries/Q1/Q1_sqlite.sql

compare_pairs:
  - [ Q1, duckdb ]
  - [ Q1, sqlite ]
```

### 4. Run Benchmarks

```bash
python run_experiments.py
```

### 5. Generate Visualizations

```bash
python analyze_results.py
```

## Configuration

All experiments are configured through `config.yaml`. **No command-line arguments needed for normal use.**

### Core Parameters

| Parameter      | Description                                     | Default            |
| -------------- | ----------------------------------------------- | ------------------ |
| `engines`      | Database engines to benchmark (🚧 Note: chDB TODO) | `[duckdb, sqlite]` |
| `repeat_pilot` | Pilot runs for interval calculation (Stage 1/2) | `3`                |
| `sample_count` | Target monitoring samples per query             | `10`               |
| `std_repeat`   | Benchmark iterations (Stage 2/2)                | `5`                |
| `output_cwd`   | Results output directory                        | `./results`        |

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

```yaml
# Execution parameters
# 🚧 Note: Only duckdb and sqlite are supported. chDB support is TODO
engines: [duckdb, sqlite]
repeat_pilot: 3        # 3 pilot runs for interval calculation
sample_count: 20       # Aim for 20 monitoring samples
std_repeat: 5          # 5 benchmark iterations
output_cwd: ./results # Output directory for results

# Engine paths
engine_paths:
  duckdb: duckdb
  sqlite: /usr/local/bin/sqlite3

# Datasets
datasets:
  - name: vs14
    duckdb_db: ./db_vs14/vs14_data.duckdb
    sqlite_db: ./db_vs14/vs14_data.sqlite

# Query groups
query_groups:
  - id: Q1_aggregation
    duckdb_sql: ./queries/Q1/Q1_duckdb.sql
    sqlite_sql: ./queries/Q1/Q1_sqlite.sql

  - id: Q2_anomaly
    duckdb_sql: ./queries/anomaly/Q2_duckdb.sql
    sqlite_sql: ./queries/anomaly/Q2_sqlite.sql

# Visualization pairs
compare_pairs:
  - [ Q1_aggregation, duckdb ]
  - [ Q1_aggregation, sqlite ]
  - [ Q2_anomaly, duckdb ]
  - [ Q2_anomaly, sqlite ]
```

See detailed comments in `config.yaml` for more information.

## Workflow

### Complete Workflow Example

#### Step 1: Prepare Data

```bash
# Ensure raw CSV files are in place
ls raw_data/acc/acc_vs14.csv
ls raw_data/hrm/hrm_vs14.csv
# ... etc

# Create in benchmark/ directory
mkdir -p db_vs14
```

#### Step 2: Create Databases

```bash
# DuckDB
python create_db.py vs14 ./db_vs14/vs14_data.duckdb --engine duckdb

# SQLite
python create_db.py vs14 ./db_vs14/vs14_data.sqlite --engine sqlite

# Optional: Create with post-SQL (indexes, etc.)
python create_db.py vs14 ./db_vs14/vs14_data.duckdb \
  --engine duckdb \
  --post-sql ./queries/create_indexes.sql
```

#### #### Step 3: Configure Experiments

Edit `config.yaml`:

1. Add your datasets under `datasets:`
2. Add your queries under `query_groups:`
3. Define comparison pairs under `compare_pairs:`

#### Step 4: Run Benchmarks

```bash
python run_experiments.py
```

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

#### Step 5: Generate Visualizations

```bash
python analyze_results.py
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

If using custom builds or specific versions:

```yaml
engine_paths:
  duckdb: /usr/local/bin/duckdb
  sqlite: /Users/me/custom-sqlite/sqlite3
```

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
