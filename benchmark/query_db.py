#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
query_db.py — Execute SQL queries with SQLite / DuckDB / chDB,
measuring TTFR (time-to-first-result), row counts, etc.
"""

import sqlite3
import duckdb
import time


def _is_select_statement(stmt: str) -> bool:
    s = stmt.lstrip().lower()
    return s.startswith(("select", "with", "show", "describe", "explain"))


def _execute_select_statement(cursor_or_conn, stmt: str):
    """
    Execute a SELECT using DB-API style fetchmany to compute TTFR and row count.
    Works for SQLite cursor and DuckDB connection.
    """
    t_start = time.perf_counter()
    
    # Handle different cursor types
    if hasattr(cursor_or_conn, "fetchmany"):  # SQLite cursor
        cursor_or_conn.execute(stmt)
        cur = cursor_or_conn
    else:  # DuckDB connection returns a cursor
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
    else:
        raise RuntimeError("Unsupported connection type")
    
    return "OK"


def _run_statements_duckdb(
    db_path: str,
    statements: list,
    threads: int | None = None
):
    """
    Execute statements using DuckDB engine.

    Returns:
        tuple: (first_select_ttfr, rows_returned, statements_executed, select_statements, retval)
    """

    rows_returned = 0
    first_select_ttfr = None
    statements_executed = 0
    select_statements = 0
    retval = None

    # Build connection config (only pass when non-empty to avoid TypeError)
    config: dict[str, int] = {}
    if threads is not None and threads > 0:
        # DuckDB expects the key 'threads'
        config["threads"] = int(threads)

    if config:
        con = duckdb.connect(database=db_path, config=config)
    else:
        # Do NOT pass config=None for older DuckDB builds
        con = duckdb.connect(database=db_path)

    try:
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


def _run_statements_sqlite(db_path: str, statements: list):
    """
    Execute statements using SQLite engine.
    
    Args:
        db_path: Path to SQLite database
        statements: List of SQL statements
        
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


def _run_statements_chdb(db_path, statements: list, threads: int | None = None, arraysize: int = 8192):
    """
    Run SQL statements on chDB via DB-API (connect/cursor), returning the same exec_info shape
    as the original _run_statements_chdb (Session-based).
    """
    # Delay the import to prevent the parent process from touching chdb prematurely
    import chdb

    # Connection target: If db_path is provided, persist to the specified directory/file; otherwise, use an in-memory database.
    conn = chdb.connect(db_path if db_path else ":memory:")
    try:
        cur = conn.cursor()
        if threads and int(threads) > 0:
            cur.execute(f"SET max_threads = {int(threads)}")

        statements_executed = 0
        select_statements = 0
        first_select_ttfr = None
        rows_returned = 0
        retval = None

        for stmt in statements:
            statements_executed += 1
            if _is_select_statement(stmt):
                select_statements += 1
                t0 = time.perf_counter()
                cur.execute(stmt)
                # The first data acquisition is used for TTFR; subsequently, batch full-scale counting is carried out.
                first_batch = cur.fetchmany(arraysize)
                ttfr = time.perf_counter() - t0
                if first_select_ttfr is None:
                    first_select_ttfr = ttfr

                rows = 0 if not first_batch else len(first_batch)
                while True:
                    batch = cur.fetchmany()
                    if not batch:
                        break
                    rows += len(batch)

                rows_returned = rows
                retval = rows_returned
            else:
                cur.execute(stmt)
                retval = "OK"

        cur.close()
        return first_select_ttfr, rows_returned, statements_executed, select_statements, retval
    finally:
        try:
            conn.close()
        except Exception:
            pass


def run_query_with_ttfr(engine: str, db_path: str | None, query: str,
                        threads: int | None = None):
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
    """
    if db_path is None:
        raise ValueError("db_path must be provided for non-in-memory databases")
    
    # Parse semicolon-separated statements
    stmts = [s.strip() for s in query.split(";") if s.strip()]

    # Execute statements using the appropriate engine
    if engine == "duckdb":
        first_select_ttfr, rows_returned, statements_executed, select_statements, retval = \
            _run_statements_duckdb(db_path, stmts, threads)
    elif engine == "sqlite":
        first_select_ttfr, rows_returned, statements_executed, select_statements, retval = \
            _run_statements_sqlite(db_path, stmts)
    elif engine == "chdb":
        first_select_ttfr, rows_returned, statements_executed, select_statements, retval = \
            _run_statements_chdb(db_path, stmts, threads)
    else:
        raise ValueError(f"Unsupported engine: {engine}")

    return {
        "retval": retval,
        "first_select_ttfr_seconds": first_select_ttfr,
        "rows_returned": rows_returned,
        "statements_executed": statements_executed,
        "select_statements": select_statements,
    }
