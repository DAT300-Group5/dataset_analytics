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
| `--duckdb-cmd` | `duckdb` | Path to DuckDB executable |
| `--sql-file` | `demo.sql` | Path to SQL script file |
| `--db-file` | `demo.db` | Path to database file |
| `--profiling-output` | `profiling_output.json` | Path to profiling JSON output |
| `--results-file` | `results.json` | Path to save results JSON |
| `--no-cpu-monitor` | - | Disable CPU monitoring |

## Examples

### Basic Usage

```bash
# Run with default settings (demo.sql)
python3 run_demo.py

# Use custom DuckDB binary
python3 run_demo.py --duckdb-cmd /path/to/duckdb

# Custom SQL file and database
python3 run_demo.py --sql-file my_queries.sql --db-file my_database.db

# Disable CPU monitoring
python3 run_demo.py --no-cpu-monitor
```

### Full Example

```bash
python3 run_demo.py \
    --duckdb-cmd /usr/local/bin/duckdb \
    --sql-file demo.sql \
    --db-file test.db \
    --profiling-output profiling.json \
    --results-file metrics.json
```

## Output Files

- **`results/profiling_query_*.json`** - DuckDB profiling output per query
- **`results.json`** - Aggregated performance metrics in JSON format
- **`demo.db`** - DuckDB database file (created during execution)

## Requirements

- Python 3.7+
- `psutil` package (for CPU monitoring)
- DuckDB CLI

## Notes

- The SQL script should include `PRAGMA enable_profiling='json'` to enable profiling
- Profiling files are automatically generated in the `results/` directory
- Each query generates a separate `profiling_query_N.json` file
