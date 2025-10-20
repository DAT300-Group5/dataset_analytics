# chdb_cli - ChDB Command Line Tool

A ChDB command line tool that reads SQL from stdin and outputs CSV with headers and query statistics.

## Prerequisites

Before building, you need to install the libchdb library.

### Install libchdb

Run the following command to install the chDB C/C++ library:

```bash
curl -sL https://lib.chdb.io | bash
```

This will automatically download and install libchdb to your system path (typically `/usr/local/lib`).

For more details, refer to the official documentation: [chDB C/C++ Installation Guide](https://clickhouse.com/docs/chdb/install/c)

## Build

After installing libchdb, compile the command line tool:

```bash
g++ -o chdb_cli chdb_cli.cpp -lchdb -L/usr/local/lib
```

## Usage

### Basic Syntax

```bash
chdb_cli <dbpath> [options] < input.sql
```

### Options

- `<dbpath>` - Database directory path (required)
- `-v, --verbose` - Show query statistics to stdout (optional)
- `-m, --memory` - Show peak memory usage to stdout (optional)

### Examples

#### Basic usage

```bash
export DYLD_LIBRARY_PATH=/usr/local/lib:$DYLD_LIBRARY_PATH
./chdb_cli ../db_vs14/vs14_data_chdb < ../queries/Q1/Q1_chdb.sql
```

Output format:

```bash
"deviceId","minute_ts","avg_hr","avg_ppg","rms_acc","total_steps","median_light"
"vs14","2021-03-03T12:37:00+00:00",51.34146341463415,2065123.2184466019,9.810548019218292,0,0
"vs14","2021-03-03T12:38:00+00:00",132.8,2067702.9197324414,9.810603630641637,0,0
"vs14","2021-03-03T12:39:00+00:00",96.84745762711864,2215788.737458194,9.815128849033968,0,596
```

#### With both verbose and memory profiling

```bash
export DYLD_LIBRARY_PATH=/usr/local/lib:$DYLD_LIBRARY_PATH
./chdb_cli ../db_vs14/vs14_data_chdb -v -m < ../queries/Q1/Q1_clickhouse.sql
```

Output format:

```bash
Query statistics:
  Elapsed: 0.384711 seconds
  Rows read: 25051
  Bytes read: 2179437 bytes
  Output rows: 25051
Peak memory: 534.672 MB
"deviceId","minute_ts","avg_hr","avg_ppg","rms_acc","total_steps","median_light"
"vs14","2021-03-03T12:37:00+00:00",51.34146341463415,2065123.2184466019,9.810548019218292,0,0
"vs14","2021-03-03T12:38:00+00:00",132.8,2067702.9197324414,9.810603630641637,0,0
"vs14","2021-03-03T12:39:00+00:00",96.84745762711864,2215788.737458194,9.815128849033968,0,596
```

## Notes

1. On macOS, set `DYLD_LIBRARY_PATH=/usr/local/lib` before running
2. Output format is **CSVWithNames** (CSV with column headers)
3. Error messages go to stderr, won't interfere with output
4. Statistics output (`-v` and `-m`) go to stdout before CSV data
