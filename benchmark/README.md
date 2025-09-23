# Database Performance Benchmark

This directory contains tools for benchmarking and comparing the performance of different database engines (DuckDB vs SQLite) for analytical queries.

## Overview

The benchmark suite provides:
- **Performance measurement tools** for CPU and memory usage
- **Query modules** for DuckDB and SQLite
- **Database creation utilities** for test data
- **Comparative analysis** between different database engines

## File Structure

```
benchmark/
├── README.md           # This documentation
├── benchmark.py        # Main benchmarking tool
├── query_duckdb.py    # DuckDB query module
├── query_sqlite.py    # SQLite query module
├── create_db.py       # Database creation utilities
├── data_sqlite.db     # SQLite test database
└── data_duckdb.db     # DuckDB test database (when created)
```

## Installation

### Prerequisites

```bash
pip install psutil pandas duckdb
```

### Required Python Packages
- `psutil` - System and process utilities
- `pandas` - Data manipulation and analysis
- `duckdb` - DuckDB database engine
- `sqlite3` - SQLite database engine (built-in)
- `tracemalloc` - Memory profiling (built-in)

## Usage

### Basic Benchmarking

Run performance benchmark with default settings (DuckDB):
```bash
python benchmark.py
```

Compare SQLite vs DuckDB:
```bash
# Test SQLite
python benchmark.py --engine sqlite

# Test DuckDB
python benchmark.py --engine duckdb
```

### Advanced Options

```bash
# Custom sampling interval (default: 0.2s)
python benchmark.py --engine duckdb --interval 0.1

# Get help
python benchmark.py --help
```

### Output Explanation

The benchmark outputs the following metrics:

```
Running query using DUCKDB engine...
[DUCKDB] wall=0.410s  peak_rss=89.9 MB  cpu_avg=51.0%  py_heap_peak=1.5 MB  samples=2
```

- **wall**: Wall clock time (seconds)
- **peak_rss**: Peak resident set size (physical memory in MB)
- **cpu_avg**: Average CPU usage percentage
- **py_heap_peak**: Peak Python heap memory (MB)
- **samples**: Number of monitoring samples taken

## Performance Characteristics

### DuckDB vs SQLite Comparison

| Metric | DuckDB | SQLite | DuckDB Advantage |
|--------|--------|--------|------------------|
| **Query Speed** | ~0.4s | ~2.3s | **5.75x faster** |
| **Memory Usage** | ~89 MB | ~86 MB | 3 MB more (3.5%) |
| **CPU Efficiency** | ~51% | ~93% | Lower CPU load |
| **Use Case** | OLAP/Analytics | OLTP/Transactions | - |

### Why DuckDB is Faster

1. **Vectorized Execution Engine**
   - Processes 1024-2048 rows at once
   - Utilizes CPU SIMD instructions
   - Reduces function call overhead

2. **Columnar Storage**
   - Optimized for analytical queries
   - Better compression and cache efficiency
   - Ideal for aggregation operations

3. **Advanced Query Optimizer**
   - Modern optimization techniques
   - Parallel execution support
   - Intelligent query planning

4. **Memory Strategy**
   - Aggressive memory allocation
   - Pre-allocated buffers
   - Cache-friendly data access patterns

## Query Modules

### DuckDB Module (`query_duckdb.py`)

Features:
- Connects to DuckDB database
- Executes analytical queries
- Optimized for aggregation operations
- Utilizes DuckDB's vectorized engine

### SQLite Module (`query_sqlite.py`)

Features:
- Connects to SQLite database
- Executes transactional queries
- Row-by-row processing
- ACID compliance

## Database Creation

Use `create_db.py` to create test databases from CSV data:

```python
from create_db import create

# Create SQLite database
create('source.csv', 'data_sqlite.db', engine='sqlite')

# Create DuckDB database
create('source.csv', 'data_duckdb.db', engine='duckdb')
```

## Monitoring Details

The benchmark tool monitors:

1. **Memory Usage**
   - Peak RSS (Resident Set Size)
   - Python heap allocation
   - Real-time memory tracking

2. **CPU Performance**
   - Average CPU percentage
   - Multi-core utilization
   - Sampling over time

3. **Execution Time**
   - Wall clock time
   - High-precision timing
   - Performance counters

## Use Cases

### Choose DuckDB when:
- Performing data analysis and aggregation queries
- Processing large datasets for statistics
- Memory is sufficient and speed is priority
- Data warehouse, BI, or scientific computing scenarios

### Choose SQLite when:
- Performing transactional operations (CRUD)
- Memory-constrained environments
- High concurrency support needed
- Mobile apps, embedded systems, simple web applications

## Technical Implementation

### Vectorized vs Row-based Processing

**DuckDB Approach:**
```
1. Allocate large memory buffer
2. Batch read 1024 rows to buffer
3. Vectorized computation: sum_vec = simd_add(values)
4. Repeat until all data processed
5. Final calculation: average = total_sum / total_count
→ More memory, faster computation
```

**SQLite Approach:**
```
1. Allocate small buffer
2. Read data row by row
3. Row-wise computation: sum += value, count += 1
4. Process each row individually
5. Final calculation: average = sum / count
→ Less memory, slower computation
```

## Performance Tips

1. **For DuckDB:**
   - Ensure sufficient memory is available
   - Use columnar-friendly query patterns
   - Leverage vectorized operations

2. **For SQLite:**
   - Create appropriate indexes
   - Use prepared statements
   - Optimize for row-wise access patterns

3. **General:**
   - Warm up databases before benchmarking
   - Run multiple iterations for average results
   - Consider data size and query complexity

## New Database Engines

When adding new database engines:

1. Create `query_[engine].py` module
2. Implement `run_query_[engine]()` function
3. Add engine choice to `benchmark.py`
4. Update this README with new comparisons
