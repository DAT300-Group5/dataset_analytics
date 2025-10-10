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
| `--no-process-monitor` | - | Disable process resource monitoring |
| `--monitor-interval` | `0.1` | Process sampling interval in seconds |

## Examples

### Basic Usage

```bash
# Run with default settings (demo.sql)
python3 run_demo.py

# Use custom SQLite binary
python3 run_demo.py --sqlite-cmd /path/to/sqlite3

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
    --output-log results/output.log \
    --json-output results/metrics.json \
    --sqlite-cmd /Users/username/sqlite3/bin/sqlite3 \
    --monitor-interval 0.05
```

## Output Files

- **`results/output.log`** - Raw SQLite output with timing information
- **`results.json`** - Performance metrics in JSON format
- **`demo.db`** - SQLite database file (created during execution)

## Sample Output

```
============================================================
Executing SQL script: demo.sql
Database: demo.db
Output log: output.log
Process monitoring: Enabled (sampling every 0.1s)
============================================================

✓ Process monitoring started for PID 12345
✓ SQL execution completed successfully
✓ Output log created: output.log (2048 bytes)
✓ Process monitoring completed (150 samples)

============================================================
Parsing output log: output.log
============================================================

Summary:
  Total queries: 5

Timing:
  Total run time: 12.3456 seconds
  Average run time: 2.4691 seconds
  Min run time: 0.1234 seconds
  Max run time: 5.6789 seconds
  Total user time: 11.2345 seconds
  Total system time: 1.1111 seconds

Memory:
  Average memory used: 524288000 bytes (512000.00 KB)
  Max memory used: 1073741824 bytes (1048576.00 KB)
  Average heap usage: 262144000 bytes (256000.00 KB)
  Max heap usage: 524288000 bytes (512000.00 KB)

Throughput:
  Total output rows: 1000000
  Overall throughput: 80971.66 rows/sec

  Last query performance:
    Output rows: 500000
    Execution time: 5.6789 seconds
    Throughput: 88024.53 rows/sec

Process Resource Usage (sampled):
  Process duration: 12.5000 seconds
  Peak CPU: 95.23%
  Average CPU: 75.45%
  Min CPU: 12.34%
  Samples collected: 150
  Peak memory (RSS): 1234.56 MB

✓ Results saved to: results.json
```

## Requirements

- Python 3.7+
- `psutil` package (for process resource monitoring)
- Custom-compiled SQLite3 with `.timer on` support

## Notes

- The SQL script should include `.timer on` and `.output` commands
- System `sqlite3` may not support proper timer output formatting
- Use `--sqlite-cmd` to specify a custom-compiled SQLite3 binary
