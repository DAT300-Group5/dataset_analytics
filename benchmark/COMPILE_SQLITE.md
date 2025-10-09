# Use Complete Dot Command in SQLite CLI

## Dot Command

Inside the SQLite shell, you can enable various diagnostic and profiling features to analyze query performance:

### 1. `.timer on` - Enable Query Timing

```sql
.timer on
```

**Purpose:** Measures and displays the execution time of each SQL statement.

**Output Example:**

```shell
Run Time: real 0.123 user 0.098000 sys 0.025000
```

- `real`: Wall-clock time (actual elapsed time)
- `user`: CPU time spent in user mode
- `sys`: CPU time spent in kernel/system mode

**Use Case:** Essential for benchmarking queries and identifying slow operations.

### 2. `.scanstatus on` - Enable Scan Status Reporting

```sql
.scanstatus on
```

**Purpose:** Provides detailed statistics about table/index scans during query execution.

**Requirements:** Only works when SQLite is compiled with `-DSQLITE_ENABLE_STMT_SCANSTATUS` flag.

**Output Information:**

- Number of rows visited in each loop
- Number of rows examined vs. returned
- Which indexes were used
- Loop nesting levels

**Output Example:**

```shell
Loop Rows Visited  Rows Examined
  0           1000           1000
  1              5              5
```

**Use Case:** Deep dive into query execution to understand:

- How many rows were scanned vs. filtered
- Index effectiveness
- Join performance bottlenecks

### 3. `.eqp full` - Display Query Execution Plan

```sql
.eqp full
```

**Purpose:** Shows the query planner's strategy for executing SQL statements.

**Options:**

- `.eqp off` - Disable execution plan display
- `.eqp on` - Show basic execution plan
- `.eqp full` - Show detailed execution plan with trigger information

**Output Example:**

```shell
QUERY PLAN
|--SCAN TABLE users
|--SEARCH TABLE orders USING INDEX idx_user_id (user_id=?)
`--USE TEMP B-TREE FOR ORDER BY
```

**Use Case:** Understanding:

- Whether indexes are being used
- Query optimization opportunities
- Table scan vs. index scan decisions
- Sort and temporary table usage

### 4. `.stat on` - Display SQLite Statistics

```sql
.stat on
```

**Purpose:** Shows internal SQLite statistics after each query execution.

**Output Information:**

- Memory usage (current and peak)
- Number of page cache hits/misses
- Number of disk reads/writes
- Parser memory usage

**Output Example:**

```shell
Memory Used:          12345 bytes
Number of Heap Allocations: 234
Page Cache Hits:      1000
Page Cache Misses:    5
```

**Use Case:** Analyzing:

- Memory consumption patterns
- Cache efficiency
- I/O performance
- Memory allocation overhead

### Additional Useful Commands

```sql
.headers on          -- Show column headers in output
.mode column         -- Display results in column format
.width auto          -- Auto-adjust column widths
.output file.txt     -- Redirect output to file
.schema tablename    -- Show table schema
.indexes tablename   -- Show indexes for a table
```

## How to Complile SQLite

**However, some of the dot commands need another build of SQLite3**.

| Command          | Purpose                       | Requires Special Compilation            |
| ---------------- | ----------------------------- | --------------------------------------- |
| `.timer on`      | Measure query execution time  | No                                      |
| `.scanstatus on` | Show detailed scan statistics | Yes (`-DSQLITE_ENABLE_STMT_SCANSTATUS`) |
| `.eqp full`      | Display query execution plan  | No                                      |
| `.stat on`       | Show memory and cache stats   | No                                      |

1. Download the SQLite source code from the official website: <https://www.sqlite.org/download.html>
2. Extract the downloaded file to a directory of your choice.
3. Open a terminal and navigate to the directory where you extracted the SQLite source code.
4. Run the following commands to compile SQLite:

    ```bash
    ./configure CFLAGS="-O2 -DSQLITE_ENABLE_STMT_SCANSTATUS" --prefix=<your_installation_path>
    make
    make install
    ```

After running these commands, SQLite will be compiled and installed in the specified installation path. You should see dirctory tree like below:

```shell
<your_installation_path>/
├── bin
│   └── sqlite3
├── include
│   └── sqlite3.h
├── lib
│   └── libsqlite3.a
└── share
```

### How to Use

Execute the following command to launch the compiled SQLite:

```bash
<your_installation_path>/bin/sqlite3 [database_file]
```

### Use your own SQLite in Python

To use your own compiled SQLite in Python, you can set the `LD_LIBRARY_PATH` environment variable to point to the directory where the SQLite shared library is located. You can do this by running the following command in your terminal:

```bash
export LD_LIBRARY_PATH=<your_installation_path>/lib:$LD_LIBRARY_PATH
```

## Complete Setup Example

Here's a complete example of setting up SQLite for performance analysis:

```bash
# Start SQLite with your database
<your_installation_path>/bin/sqlite3 mydata.db

# Enable all diagnostic features
SQLite version 3.50.4
sqlite> .timer on
sqlite> .scanstatus on
sqlite> .eqp full
sqlite> .stat on

# Now run your query
sqlite> SELECT * FROM users WHERE age > 25;

# You'll see comprehensive output:
# - Query execution plan
# - Scan statistics
# - Timing information
# - Memory and cache statistics
```
