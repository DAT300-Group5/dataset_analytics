import sqlite3
import duckdb
import time


def _is_select_statement(stmt: str) -> bool:
    s = stmt.lstrip().lower()
    return s.startswith("select") or s.startswith("with")


def _execute_select_statement(cursor_or_conn, stmt: str):
    """
    Execute a SELECT statement and calculate TTFR (Time To First Row).
    
    Args:
        cursor_or_conn: Database cursor or connection
        stmt: SQL SELECT statement
        
    Returns:
        tuple: (ttfr_seconds, rows_returned)
    """
    t_start = time.perf_counter()
    
    # Handle different cursor types
    if hasattr(cursor_or_conn, 'fetchmany'):  # SQLite cursor
        cursor_or_conn.execute(stmt)
        cur = cursor_or_conn
    else:  # DuckDB connection
        cur = cursor_or_conn.execute(stmt)
    
    first_batch = cur.fetchmany(10000)
    t_first = time.perf_counter()
    ttfr = t_first - t_start
    
    count = len(first_batch)
    while True:
        batch = cur.fetchmany(10000)
        if not batch:
            break
        count += len(batch)
    
    return ttfr, count


def _execute_non_select_statement(cursor_or_conn, stmt: str):
    """
    Execute a non-SELECT statement.
    
    Args:
        cursor_or_conn: Database cursor or connection
        stmt: SQL statement (non-SELECT)
        
    Returns:
        str: "OK" to indicate successful execution
    """
    if hasattr(cursor_or_conn, 'execute') and hasattr(cursor_or_conn, 'fetchmany'):  # SQLite cursor
        cursor_or_conn.execute(stmt)
    else:  # DuckDB connection
        cursor_or_conn.execute(stmt)
    
    return "OK"


def _run_statements_duckdb(db_path: str, statements: list, duckdb_threads: int | None = None):
    """
    Execute statements using DuckDB engine.
    
    Args:
        db_path: Path to DuckDB database
        statements: List of SQL statements
        duckdb_threads: Number of threads for DuckDB (optional)
        
    Returns:
        tuple: (first_select_ttfr, rows_returned, statements_executed, select_statements, retval)
    """
    
    rows_returned = 0
    first_select_ttfr = None
    statements_executed = 0
    select_statements = 0
    retval = None
    
    con = duckdb.connect(db_path)
    try:
        if duckdb_threads and duckdb_threads > 0:
            con.execute(f"PRAGMA threads={duckdb_threads};")

        for stmt in statements:
            statements_executed += 1
            if _is_select_statement(stmt):
                select_statements += 1
                ttfr, count = _execute_select_statement(con, stmt)
                if first_select_ttfr is None:
                    first_select_ttfr = ttfr
                rows_returned = count
                retval = rows_returned
            else:
                retval = _execute_non_select_statement(con, stmt)
    finally:
        con.close()
    
    return first_select_ttfr, rows_returned, statements_executed, select_statements, retval

def _run_statements_sqlite(db_path: str, statements: list, sqlite_pragmas: dict | None = None):
    """
    Execute statements using SQLite engine.
    
    Args:
        db_path: Path to SQLite database
        statements: List of SQL statements
        sqlite_pragmas: SQLite pragma settings (optional)
        
    Returns:
        tuple: (first_select_ttfr, rows_returned, statements_executed, select_statements, retval)
    """
    
    rows_returned = 0
    first_select_ttfr = None
    statements_executed = 0
    select_statements = 0
    retval = None
    
    con = sqlite3.connect(db_path)
    try:
        con.isolation_level = None  # autocommit
        if sqlite_pragmas:
            for k, v in sqlite_pragmas.items():
                con.execute(f"PRAGMA {k}={v};")

        cur = con.cursor()
        for stmt in statements:
            statements_executed += 1
            if _is_select_statement(stmt):
                select_statements += 1
                ttfr, count = _execute_select_statement(cur, stmt)
                if first_select_ttfr is None:
                    first_select_ttfr = ttfr
                rows_returned = count
                retval = rows_returned
            else:
                retval = _execute_non_select_statement(cur, stmt)
    finally:
        con.close()
    
    return first_select_ttfr, rows_returned, statements_executed, select_statements, retval


def run_query_with_ttfr(engine: str, db_path: str, query: str,
                        duckdb_threads: int | None = None,
                        sqlite_pragmas: dict | None = None):
    """
    Execute semicolon-separated SQL statements and collect:
      - first_select_ttfr_seconds: TTFR of the FIRST SELECT (None if no SELECT)
      - rows_returned: total rows returned by the LAST SELECT (0 if none)
      - statements_executed, select_statements

    TTFR definition:
      time from execute() to fetching the first batch (fetchmany) of the SELECT.

    Notes:
      - We fetch in chunks (fetchmany(10000)) to avoid blowing memory.
      - DuckDB threads can be controlled via PRAGMA threads=<k>.
      - SQLite pragmas (journal_mode, synchronous, cache_size) can be set.
    """
    # Parse semicolon-separated statements
    stmts = [s.strip() for s in query.split(";") if s.strip()]

    # Execute statements using the appropriate engine
    if engine == "duckdb":
        first_select_ttfr, rows_returned, statements_executed, select_statements, retval = \
            _run_statements_duckdb(db_path, stmts, duckdb_threads)
    elif engine == "sqlite":
        first_select_ttfr, rows_returned, statements_executed, select_statements, retval = \
            _run_statements_sqlite(db_path, stmts, sqlite_pragmas)
    else:
        raise ValueError(f"Unsupported engine: {engine}")

    return {
        "retval": retval,
        "first_select_ttfr_seconds": first_select_ttfr,
        "rows_returned": rows_returned,
        "statements_executed": statements_executed,
        "select_statements": select_statements,
    }