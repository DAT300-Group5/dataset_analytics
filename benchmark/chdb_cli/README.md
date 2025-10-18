# chdb_cli - ChDB Command Line Tool

A ChDB command line tool that reads SQL from stdin and outputs CSV with headers.

## Build

```bash
g++ -o chdb_cli chdb_cli.cpp  -lchdb -L/usr/local/lib
```

## Usage

### Basic Syntax

```bash
chdb_cli <dbpath> [options] < input.sql > output.csv
```

### Options

- `<dbpath>` - Database directory path (required)
- `-v, --verbose` - Show query statistics to stdout (optional)
- `-m, --memory` - Show peak memory usage to stdout (optional)

### Examples

#### Basic usage
```bash
export DYLD_LIBRARY_PATH=/usr/local/lib:$DYLD_LIBRARY_PATH
./chdb_cli ../db_vs14/vs14_data_chdb < ../queries/Q1/Q1_clickhouse.sql
```


Output format:
```
"ts","deviceId","x","y","z"
"2021-03-03 12:37:18.932","vs14",8.458629,-4.900501,-1.112662
"2021-03-03 12:37:18.938","vs14",8.394022,-4.905287,-1.150948
"2021-03-03 12:37:19.138","vs14",8.379664,-4.972286,-1.129412
```

#### With both verbose and memory profiling
```bash
export DYLD_LIBRARY_PATH=/usr/local/lib:$DYLD_LIBRARY_PATH
./chdb_cli ../db_vs14/vs14_data_chdb -v -m < ../queries/Q1/Q1_clickhouse.sql
```

Output format:
```
Query statistics:
  Elapsed: 0.398 seconds
  Output rows: 25042
Peak memory: 386.969 MB
Query count: 3
"ts","deviceId","x","y","z"
"2021-03-03 12:37:18.932","vs14",8.458629,-4.900501,-1.112662
"2021-03-03 12:37:18.938","vs14",8.394022,-4.905287,-1.150948
"2021-03-03 12:37:19.138","vs14",8.379664,-4.972286,-1.129412
```


## Notes

1. On macOS, set `DYLD_LIBRARY_PATH=/usr/local/lib` before running
2. Output format is **CSVWithNames** (CSV with column headers)
3. Error messages go to stderr, won't interfere with CSV output
4. Statistics output (`-v` and `-m`) go to stdout before CSV data
