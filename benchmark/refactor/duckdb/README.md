# DuckDB Demo Runner

## Purpose

Standalone tool to execute DuckDB SQL scripts and collect performance metrics from profiling output. This tool runs independently from the unified benchmark interface and is useful for quick testing and debugging.

## Usage

```bash
python3 run_demo.py [OPTIONS]
```

## Command Line Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--sql-file` | `demo.sql` | Path to the SQL script file |
| `--db-file` | `demo.db` | Path to the DuckDB database file |
| `--profiling-output` | `profiling_output.json` | Path to the profiling output file |
| `--json-output` | `results.json` | Path to save JSON results |
| `--duckdb-cmd` | `duckdb` | DuckDB command or full path |
| `--no-save` | - | Don't save results to JSON file |
| `--no-process-monitor` | - | Disable process resource monitoring |
| `--monitor-interval` | `0.1` | Process sampling interval in seconds |

## Examples

### Basic Usage

```bash
# Run with default settings (demo.sql)
python3 run_demo.py

# Use custom DuckDB binary
python3 run_demo.py --duckdb-cmd /path/to/duckdb

# Custom SQL file and database
python3 run_demo.py --sql-file my_queries.sql --db-file my_database.db

# Disable process monitoring for faster execution
python3 run_demo.py --no-process-monitor

# Don't save JSON results
python3 run_demo.py --no-save
```

### Full Example

```bash
python3 run_demo.py \
    --sql-file demo.sql \
    --db-file test.db \
    --profiling-output profiling.json \
    --json-output results/metrics.json \
    --duckdb-cmd /usr/local/bin/duckdb \
    --monitor-interval 0.05
```

## Output Files

- **`results/profiling_query_*.json`** - DuckDB profiling output per query
- **`results.json`** - Aggregated performance metrics in JSON format
- **`demo.db`** - DuckDB database file (created during execution)

## Sample Output

```
============================================================
Executing SQL script: demo.sql
Database: demo.db
Profiling output: results/profiling_query_*.json
Process monitoring: Enabled (sampling every 0.1s)
============================================================

✓ Process monitoring started for PID 12345
✓ SQL execution completed successfully
✓ Profiling files created: 5 file(s)
✓ Process monitoring completed (125 samples)

============================================================
Parsing profiling output: 5 file(s)
============================================================

Summary:
  Total queries: 5

Timing:
  Total wall time: 10.2345 seconds
  Average wall time: 2.0469 seconds
  Min wall time: 0.0987 seconds
  Max wall time: 4.5678 seconds

Memory:
  Average memory used: 419430400 bytes (409600.00 KB)
  Peak memory used: 838860800 bytes (819200.00 KB)
  Peak memory used (MB): 800.00 MB

Throughput:
  Total output rows: 1000000
  Overall throughput: 97705.55 rows/sec

  Last query performance:
    Output rows: 500000
    Execution time: 4.5678 seconds
    Throughput: 109456.78 rows/sec

Process Resource Usage (sampled):
  Process duration: 10.3000 seconds
  Peak CPU: 98.12%
  Average CPU: 82.34%
  Min CPU: 15.67%
  Samples collected: 103
  Peak memory (RSS): 987.65 MB

✓ Results saved to: results.json
```

## Requirements

- Python 3.7+
- `psutil` package (for process resource monitoring)
- DuckDB CLI

## Notes

- The SQL script should include `PRAGMA enable_profiling='json'` to enable profiling
- Profiling files are automatically generated in the `results/` directory
- Each query generates a separate `profiling_query_N.json` file
