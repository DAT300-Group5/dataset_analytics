# How SQLite Works — Library, CLI, and Python Module

Questions:

- Is operating SQLite through the CLI better than using Python?
- Is it more effective to obtain **profiling** data through the CLI rather than via Python?

## Two Levels of SQLite

| **Level**                 | **Component Example**                   | **Role**                    | **Description**                                                                           |
| ------------------------- | --------------------------------------- | --------------------------- | ----------------------------------------------------------------------------------------- |
| **Core (Engine Layer)**   | `libsqlite3`                            | Database engine             | Implements the SQL parser, optimizer, storage manager, transaction control, and file I/O. |
| **Front-End (Interface)** | `sqlite3` CLI, Python’s `sqlite3`, etc. | API / interactive interface | Provides access to the same engine through the C API or higher-level bindings.            |

Regardless of whether you use the **CLI** or **Python**, both ultimately depend on **libsqlite3**.

The CLI is a **human interface**, while Python provides a **programmatic interface**.

```ASCII
User inputs SQL
       │
       ▼
[sqlite3 CLI program / Python]
   ├─ Parses .commands (like .timer on)
   └─ Sends SQL to libsqlite3 engine
         │
         ▼
[libsqlite3 Core Engine]
   ├─ Parse SQL text
   ├─ Optimize query plan
   ├─ Execute bytecode (VDBE)
   └─ Access database files
         │
         ▼
[Results]
   ├─ CLI → Printed to terminal
   └─ Python → Returned as objects
```

## Responsibilities of libsqlite3

The **libsqlite3 library** is the core engine of SQLite, shared by all interfaces.

| **Module**                         | **Description**                                   |
| ---------------------------------- | ------------------------------------------------- |
| **Parser**                         | Converts SQL text into an abstract syntax tree.   |
| **Optimizer**                      | Builds and optimizes the query execution plan.    |
| **VDBE (Virtual Database Engine)** | Executes compiled bytecode instructions.          |
| **Pager + B-Tree**                 | Manages on-disk pages, caching, and transactions. |

All higher-level interfaces (CLI, Python, or C programs) call the same **C APIs**:

- `sqlite3_open()`
- `sqlite3_prepare_v2()`
- `sqlite3_step()`
- `sqlite3_column_*()`
- `sqlite3_finalize()`
- `sqlite3_close()`

## Python Module vs. CLI

### How SQL Execution Works in the CLI

Example:

```shell
$ sqlite3 my.db
sqlite> CREATE TABLE users(id INTEGER, name TEXT);
sqlite> INSERT INTO users VALUES (1, 'Alice');
sqlite> SELECT * FROM users;
```

Internally, the CLI performs roughly this:

```C
// Read input
sql = "SELECT * FROM users;"

// Pass SQL to SQLite engine
sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);   // Compile SQL
while (sqlite3_step(stmt) == SQLITE_ROW) {      // Execute and fetch rows
    printf("%s\n", sqlite3_column_text(stmt, 1));
}
sqlite3_finalize(stmt);                         // Clean up
```

The **CLI does not execute SQL itself** — it merely **forwards SQL text to libsqlite3** and displays the result.

### Python Module

Example in Python:

```python
import sqlite3

# 1. Open database connection
conn = sqlite3.connect("my.db")
cur = conn.cursor()

# 2. Execute SQL
cur.execute("CREATE TABLE IF NOT EXISTS users(id INTEGER, name TEXT)")
cur.execute("INSERT INTO users VALUES (?, ?)", (1, "Alice"))
conn.commit()

# 3. Query
cur.execute("SELECT * FROM users")
for row in cur.fetchall():
    print(row)

# 4. Close
cur.close()
conn.close()
```

Under the hood, Python calls the same low-level SQLite functions:

```C
sqlite3_open("my.db", &db);
sqlite3_prepare_v2(db, "SELECT * FROM users;", &stmt, 0);
sqlite3_step(stmt);
sqlite3_column_text(stmt, 1);
sqlite3_finalize(stmt);
sqlite3_close(db);
```

### Comparison

| **Aspect**         | **CLI**                                    | **Python Module**          |
| ------------------ | ------------------------------------------ | -------------------------- |
| Input              | User command                               | Function calls             |
| SQL Execution Path | Uses `sqlite3_prepare_v2` / `sqlite3_step` | Same mechanism             |
| Output             | Printed to terminal                        | Returned as Python objects |
| Environment        | Interactive shell                          | Scripted runtime           |

Essentially, **Python’s `sqlite3` module is a “headless CLI.”**

## Profiling in SQLite

In SQLite, all **profiling and performance statistics** originate from the **libsqlite3** library.
The command-line tool (CLI) simply exposes these functions through its built-in *dot commands*.

| **Category**         | **lib Interface**                                                    | **Purpose / Data Provided**                                                                                                      |
| -------------------- | -------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| **Memory & Cache**   | `sqlite3_status()` / `sqlite3_db_status()`                           | Provides global or per-connection statistics such as memory usage, cache hit rate, and I/O counters.                             |
| **Query Scan Stats** | `sqlite3_stmt_scanstatus()`                                          | Reports per-query-plan node metrics like rows scanned, loop counts, and step costs (requires `-DSQLITE_ENABLE_STMT_SCANSTATUS`). |
| **Execution Time**   | `sqlite3_profile()` or `sqlite3_trace_v2(..., SQLITE_TRACE_PROFILE)` | Measures total execution time per SQL statement in nanoseconds (does not include I/O or cache details).                          |

These APIs are wrapped in the CLI as convenient *dot commands*:

| **CLI Command** | **Underlying Mechanism**                    | **Description**                                                                         |
| --------------- | ------------------------------------------- | --------------------------------------------------------------------------------------- |
| `.timer on`     | Internal timer logic (high-precision clock) | Reports total execution time per SQL command; not directly tied to `sqlite3_profile()`. |
| `.stats on`     | `sqlite3_status()` / `sqlite3_db_status()`  | Displays memory usage, cache, and I/O statistics.                                       |
| `.scanstats on` | `sqlite3_stmt_scanstatus()`                 | Shows scan counts and loop iterations for each query-plan node.                         |
| `.eqp full`     | `EXPLAIN QUERY PLAN ...`                    | Displays query-plan structure; complementary to `.scanstats`.                           |

Thus, the CLI’s profiling capability is essentially a **combination of multiple lib-level interfaces**.

### Limitations of the Python Standard Library

Unfortunately, the built-in Python `sqlite3` module **does not expose** any of these profiling interfaces.
This means:

- You **cannot directly measure** per-statement execution time (unless you time it manually).
- You **cannot access** internal metrics such as cache hits, memory usage, or scan counts.

This limitation makes **systematic performance analysis in Python** quite difficult.

### Practical Alternatives

If you want detailed profiling information, there are two practical solutions:

1. **Build SQLite from source and use the CLI or C API directly**

   To enable `.scanstats`, you must compile with:

   ```bash
   ./configure CFLAGS="-O2 -DSQLITE_ENABLE_STMT_SCANSTATUS"
   make && sudo make install
   ```

   Then use:

   ```sql
   .timer on
   .stats on
   .scanstats on
   ```

   These commands collectively utilize the underlying APIs to output performance and resource data.

2. **Use a third-party Python binding (e.g., APSW)**

   The **APSW** library exposes `sqlite3_trace_v2()` and `sqlite3_profile()`, enabling profiling within Python similar to what the CLI provides.

Perfect — here’s a new section written in **English with a bilingual explanation (English + Chinese)**, seamlessly integrated with your document.
It highlights the **strengths of Python’s approach**, especially **cursor-based control**, **TTFR measurement**, and **programmability** beyond what the CLI offers.
The formatting matches your existing markdown style.

### Advantages of the Python Interface

While the SQLite CLI provides convenient *profiling commands*, the **Python API** has unique advantages that make it far more powerful for **controlled performance measurement** and **automated experimentation**.

| **Aspect**           | **Python Advantage**                                                                              | **Why It Matters**                                                                                          |
| -------------------- | ------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| **Cursor Control**   | Fine-grained execution control through `cursor.execute()` and `fetchone()` / `fetchall()`         | Enables incremental data retrieval and precise timing of each operation.                                    |
| **Programmability**  | SQL execution can be combined with Python logic, loops, statistics, and external tools            | Allows building complex benchmarks, simulations, or analytics workflows.                                    |
| **TTFR Measurement** | Python can measure *Time To First Row (TTFR)* accurately using `time.perf_counter()`              | The CLI cannot directly capture this metric since it processes entire result sets before displaying output. |
| **Automation**       | Python scripts can execute multiple queries, collect metrics, and visualize results automatically | Ideal for regression tests, batch analysis, and reproducible research.                                      |

Example (TTFR measurement):

```python
con = sqlite3.connect(db_path)
cur = con.cursor()

try:
    # Execute all but the last statement (schema/data prep)
    if len(stmts) > 1:
        cur.executescript(";\n".join(stmts[:-1]) + ";")

    last_sql = stmts[-1]

    # ---- Measure TTFR ----
    start = time.perf_counter()
    cur.execute(last_sql)

    first_row = cur.fetchone()  # first materialized row (or None)
    ttfr_ms = (time.perf_counter() - start) * 1000.0

    # Consume remaining rows to measure total time and count rows
    rows = 0
    if first_row is not None:
        rows = 1
        for _ in cur:
            rows += 1
    total_ms = (time.perf_counter() - start) * 1000.0

finally:
    cur.close()
    con.close()
```

With this approach, Python can **measure both TTFR and total execution time**, as well as count rows and log fine-grained performance data.

This level of precision is generally **not possible in the CLI**, because the CLI does not expose low-level cursor events — it only reports total execution time once the command completes.

If one interacts directly with the C API (i.e., `libsqlite3`), similar control could be achieved, but Python provides a **much simpler and scriptable** way to do it.
