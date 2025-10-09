# How SQLite Works — Library, CLI, and Python Module

## Overview: The Three Layers of SQLite

| **Layer**                                 | **Component**           | **Role**              | **Description**                                       |
| ----------------------------------------- | ----------------------- | --------------------- | ----------------------------------------------------- |
| **Layer 1: Core Engine**                  | libsqlite3              | Database engine       | Parses, optimizes, executes SQL, and handles file I/O |
| **Layer 2: Command-Line Interface (CLI)** | sqlite3                 | Interactive shell     | Reads user input and forwards SQL to the engine       |
| **Layer 3: Language Binding**             | Python’s sqlite3 module | Programming interface | Provides API access to the same SQLite engine         |

Whether you use the CLI or Python, both ultimately run on **libsqlite3**.

The CLI is a *human interface* *programmatic interface*

## Execution Flow Overview

```ASCII
User inputs SQL
       │
       ▼
[sqlite3 CLI program]
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

## What Happens When You Run SQL in the CLI

Example:

```shell
$ sqlite3 my.db
sqlite> CREATE TABLE users(id INTEGER, name TEXT);
sqlite> INSERT INTO users VALUES (1, 'Alice');
sqlite> SELECT * FROM users;
```

Inside the CLI, the process is roughly equivalent to:

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

The **CLI itself doesn’t execute SQL** — it merely **forwards it to libsqlite3** and displays the output.

## Responsibilities of libsqlite3

The **libsqlite3 library** is the core of SQLite — shared by all interfaces.

Its main modules include:

| **Module**                         | **Description**                                  |
| ---------------------------------- | ------------------------------------------------ |
| **Parser**                         | Converts SQL text into an abstract syntax tree   |
| **Optimizer**                      | Generates and optimizes the execution plan       |
| **VDBE (Virtual Database Engine)** | Executes bytecode instructions                   |
| **Pager + B-Tree**                 | Manages on-disk pages, transactions, and caching |

All higher-level interfaces (CLI, Python, C programs) call the same core **C APIs**:

- `sqlite3_open()`
- `sqlite3_prepare_v2()`
- `sqlite3_step()`
- `sqlite3_column_*()`
- `sqlite3_finalize()`
- `sqlite3_close()`

## Python Module vs. CLI

Python example:

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

Under the hood, Python calls the same SQLite C API:

```C
sqlite3_open("my.db", &db);
sqlite3_prepare_v2(db, "SELECT * FROM users;", &stmt, 0);
sqlite3_step(stmt);
sqlite3_column_text(stmt, 1);
sqlite3_finalize(stmt);
sqlite3_close(db);
```

| **Comparison** | **CLI**                                | **Python Module**          |
| -------------- | -------------------------------------- | -------------------------- |
| Input          | User command                           | Function calls             |
| SQL execution  | Uses sqlite3_prepare_v2 / sqlite3_step | Same mechanism             |
| Output         | Printed to terminal                    | Returned as Python objects |
| Environment    | Interactive shell                      | Scripted runtime           |

In essence, **Python’s sqlite3 module is a “headless CLI”**.

## CLI-Specific .commands

| **Type**                    | **Example**                           | **Execution Layer** | **Available in Python?** |
| --------------------------- | ------------------------------------- | ------------------- | ------------------------ |
| **Dot Commands (.command)** | .timer on, .scanstats on              | CLI shell layer     | ❌ No                     |
| **SQL / PRAGMA**            | EXPLAIN QUERY PLAN, PRAGMA cache_size | Engine layer        | ✅ Yes                    |
| **C API Functions**         | sqlite3_trace_v2(), sqlite3_profile() | Library layer       | ✅ Indirectly             |

**Explanation:**

.timer, .scanstats, etc., are **features of the CLI shell**, not SQL commands.

They’re not part of the SQLite engine, which is why Python can’t execute them directly.

To analyze performance in Python, you can use:

- EXPLAIN ANALYZE
- or sqlite3.set_profile() for timing callbacks.

## Call Stack Comparison

### CLI Execution Path

```ASCII
┌──────────────────────────────┐
│ User inputs SQL              │
└──────────────┬───────────────┘
               ▼
    ┌─────────────────────────┐
    │ sqlite3 CLI Shell       │
    │ (.commands / SQL parser)│
    └──────────┬──────────────┘
               ▼
    ┌─────────────────────────┐
    │ libsqlite3 Engine       │
    │ Parse → Optimize → Run  │
    └──────────┬──────────────┘
               ▼
    ┌─────────────────────────┐
    │ Output to Terminal      │
    └─────────────────────────┘
```

### Python Execution Path

```ASCII
┌──────────────────────────────┐
│ Python Script Calls sqlite3  │
└──────────────┬───────────────┘
               ▼
    ┌─────────────────────────┐
    │ Python sqlite3 Wrapper  │
    │ Calls C-level API       │
    └──────────┬──────────────┘
               ▼
    ┌─────────────────────────┐
    │ libsqlite3 Engine       │
    │ Executes SQL            │
    └──────────┬──────────────┘
               ▼
    ┌─────────────────────────┐
    │ Returns Python Objects  │
    │ (list, tuple, etc.)     │
    └─────────────────────────┘
```

**Key takeaway:**

- CLI = interactive shell
- Python = programmatic interface
- libsqlite3 = the shared execution core

## Summary

- The SQLite CLI is essentially a **demo shell**

- the real SQL execution happens in **libsqlite3**

- Python’s sqlite3 module uses the same core engine, but provides a *programmatic* *interactive*
