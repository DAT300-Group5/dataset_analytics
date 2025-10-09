# Unified Database Benchmark Interface

## What It Does

Unified benchmarking tool for S**Output:**

```shell
🎯 5 Core Metrics:
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃                ┃      SQLite ┃      DuckDB ┃
┣━━━━━━━━━━━━━━━━╋━━━━━━━━━━━━━╋━━━━━━━━━━━━━┫
┃   1. Wall Time ┃  0.015000 s ┃  0.012711 s ┃
┣━━━━━━━━━━━━━━━━╋━━━━━━━━━━━━━╋━━━━━━━━━━━━━┫
┃ 2. Peak Memory ┃     1.07 MB ┃    36.38 MB ┃
┣━━━━━━━━━━━━━━━━╋━━━━━━━━━━━━━╋━━━━━━━━━━━━━┫
┃  3. Total Rows ┃          50 ┃          50 ┃
┣━━━━━━━━━━━━━━━━╋━━━━━━━━━━━━━╋━━━━━━━━━━━━━┫
┃  4. Throughput ┃ 3333.33 r/s ┃ 3934.00 r/s ┃
┣━━━━━━━━━━━━━━━━╋━━━━━━━━━━━━━╋━━━━━━━━━━━━━┫
┃    5. Peak CPU ┃      95.30% ┃     106.50% ┃
┗━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━┛

📊 Comparison:
  ⚡ DuckDB is 1.18x faster (wall time)
  🧠 DuckDB uses 34.0x more memory
  🚀 DuckDB has 1.18x higher throughput
```

Provides identical interface for both engines, measures 5 core performance metrics, and supports side-by-side comparison.

**5 Core Metrics:**

1. Wall Time (total execution time)
2. Peak Memory (maximum memory usage)
3. Total Output Rows (number of rows returned)
4. Overall Throughput (rows per second)
5. Peak CPU Usage (maximum CPU utilization)

## Project Structure

```shell
refactor/
├── run_benchmark.py           # CLI entry point
├── benchmark_interface.py     # Abstract base class + factory function
├── sqlite_benchmark.py        # SQLite implementation
├── duckdb_benchmark.py        # DuckDB implementation
├── utils/
│   ├── __init__.py
│   └── cpu_monitor.py        # Shared CPU monitoring (psutil)
├── sqlite/
│   ├── demo.sql              # Example SQL script
│   ├── log_parser.py         # Parse .timer on output
│   └── results/              # Runtime output directory
└── duckdb/
    ├── demo.sql              # Example SQL script
    ├── log_parser.py         # Parse profiling_query_*.json
    └── results/              # Runtime output directory
```

## Command Line Arguments

| Argument       | Required    | Description                                       |
| -------------- | ----------- | ------------------------------------------------- |
| `--engine`     | Yes         | `sqlite`, `duckdb`, or `both`                     |
| `--db`         | Yes         | Database filename (e.g., `test.db`)               |
| `--sql`        | Conditional | SQL file for single-engine mode                   |
| `--sqlite-sql` | Conditional | SQLite SQL file for comparison mode               |
| `--duckdb-sql` | Conditional | DuckDB SQL file for comparison mode               |
| `--sqlite-cmd` | Optional    | Path to `sqlite3` executable (default: `sqlite3`) |
| `--duckdb-cmd` | Optional    | Path to `duckdb` executable (default: `duckdb`)   |
| `--output`     | Optional    | JSON output file path                             |

**Notes:**

- Single-engine mode: use `--sql`
- Comparison mode (`--engine both`): use `--sqlite-sql` and `--duckdb-sql`
- System `sqlite3` may not support `.timer on`. Use custom-compiled version if needed.

## Complete Example

### Prerequisites

```bash
pip install psutil tabulate
```

### Single Engine Test

```bash
cd benchmark/refactor

python3 run_benchmark.py \
    --engine duckdb \
    --db test.db \
    --sql duckdb/demo.sql
```

### Comparison Mode (Full Command)

```bash
cd benchmark/refactor

python3 run_benchmark.py \
    --engine both \
    --db test.db \
    --sqlite-sql sqlite/demo.sql \
    --duckdb-sql duckdb/demo.sql \
    --sqlite-cmd /Users/xiejiangzhao/sqlite3/bin/sqlite3 \
    --output results.json
```

**Output:**

```shell
🎯 5 Core Metrics:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        SQLite          DuckDB
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  1. Wall Time          0.015000 s      0.012711 s
  2. Peak Memory        1.07 MB         36.38 MB
  3. Total Rows         50              50
  4. Throughput         3333.33 r/s     3933.51 r/s
  5. Peak CPU           95.30%          106.50%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 Comparison:
  ⚡ DuckDB is 1.18x faster (wall time)
  🧠 DuckDB uses 34.0x more memory
  🚀 DuckDB has 1.18x higher throughput
  
✅ Results saved to: results.json
```

## JSON Output Format

```json
{
  "results": [
    {
      "engine": "SQLite",
      "database": "test.db",
      "sql_file": "sqlite/demo.sql",
      "summary": {
        "total_queries": 7,
        "total_wall_time": 0.015,
        "peak_memory_bytes": 1122304,
        "peak_memory_mb": 1.07,
        "total_output_rows": 50,
        "overall_throughput_rows_per_sec": 3333.33,
        "peak_cpu_percent": 95.30
      }
    },
    {
      "engine": "DuckDB",
      "database": "test.db",
      "sql_file": "duckdb/demo.sql",
      "summary": {
        "total_queries": 7,
        "total_wall_time": 0.012711,
        "peak_memory_bytes": 38147584,
        "peak_memory_mb": 36.38,
        "total_output_rows": 50,
        "overall_throughput_rows_per_sec": 3933.51,
        "peak_cpu_percent": 106.50
      }
    }
  ],
  "comparison": {
    "wall_time_ratio": 0.85,
    "memory_ratio": 34.0,
    "throughput_ratio": 1.18,
    "faster_engine": "DuckDB"
  }
}
```
