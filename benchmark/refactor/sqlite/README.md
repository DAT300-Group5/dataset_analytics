# SQLite Demo Runner

## Purpose

Standalone tool to execute SQLite SQL scripts and collect performance metrics. This tool runs independently from the unified benchmark interface and is useful for quick testing and debugging.

## Usage

```bash
python3 run_demo.py [OPTIONS]
```

## Command Line Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--sql-file` | `demo.sql` | Path to the SQL script file |
| `--db-file` | `demo.db` | Path to the SQLite database file |
| `--output-log` | `results/output.log` | Path to the output log file |
| `--json-output` | `results.json` | Path to save JSON results |
| `--sqlite-cmd` | `sqlite3` | SQLite3 command or full path |
| `--no-save` | - | Don't save results to JSON file |
| `--no-cpu-monitor` | - | Disable CPU monitoring |
| `--cpu-interval` | `0.1` | CPU sampling interval in seconds |

## Examples

### Basic Usage

```bash
# Run with default settings (demo.sql)
python3 run_demo.py

# Use custom SQLite binary
python3 run_demo.py --sqlite-cmd /path/to/sqlite3

# Custom SQL file and database
python3 run_demo.py --sql-file my_queries.sql --db-file my_database.db

# Disable CPU monitoring for faster execution
python3 run_demo.py --no-cpu-monitor

# Don't save JSON results
python3 run_demo.py --no-save
```

### Full Example

```bash
python3 run_demo.py \
    --sql-file demo.sql \
    --db-file test.db \
    --output-log results/output.log \
    --json-output results/metrics.json \
    --sqlite-cmd /Users/username/sqlite3/bin/sqlite3 \
    --cpu-interval 0.05
```

## Output Files

- **`results/output.log`** - Raw SQLite output with timing information
- **`results.json`** - Performance metrics in JSON format
- **`demo.db`** - SQLite database file (created during execution)

## Requirements

- Python 3.7+
- `psutil` package (for CPU monitoring)
- Custom-compiled SQLite3 with `.timer on` support

## Notes

- The SQL script should include `.timer on` and `.output` commands
- System `sqlite3` may not support proper timer output formatting
- Use `--sqlite-cmd` to specify a custom-compiled SQLite3 binary
