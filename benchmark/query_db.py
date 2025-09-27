#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
query_db.py — Execute SQL queries with SQLite / DuckDB / chDB,
measuring TTFR (time-to-first-result), row counts, etc.
"""

import sqlite3
import duckdb
import time
from chdb import session as chs


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


def _run_statements_chdb(db_path: str, statements: list, threads: int | None = None):
    """
    Execute statements using chDB session.
    - db_path: directory path for persistent session
    - SELECT: use send_query streaming to measure TTFR and count rows
    - Non-SELECT: sess.query()
    """
    rows_returned = 0
    first_select_ttfr = None
    statements_executed = 0
    select_statements = 0
    retval = None

    sess = chs.Session(db_path) if db_path else chs.Session()
    try:
        if threads and threads > 0:
            sess.query(f"SET max_threads = {int(threads)}")
        
        # Select the sensor database where tables were created
        if db_path:
            sess.query("USE sensor")

        for stmt in statements:
            statements_executed += 1
            if _is_select_statement(stmt):
                select_statements += 1
                t_start = time.perf_counter()
                rows = 0
                first_seen = False
                with sess.send_query(stmt, "CSV") as stream:
                    for chunk in stream:
                        if not first_seen:
                            ttfr = time.perf_counter() - t_start
                            if first_select_ttfr is None:
                                first_select_ttfr = ttfr
                            first_seen = True
                        data = chunk.data()
                        if data:
                            lines = data.splitlines()
                            rows += sum(1 for _ in lines if _)
                rows_returned = rows
                retval = rows_returned
            else:
                sess.query(stmt)
                retval = "OK"
    finally:
        try:
            sess.close()
        except Exception:
            pass

    return first_select_ttfr, rows_returned, statements_executed, select_statements, retval


def run_query_with_ttfr(engine: str, db_path: str, query: str,
                        threads: int | None = None,
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
            _run_statements_duckdb(db_path, stmts, threads)
    elif engine == "sqlite":
        first_select_ttfr, rows_returned, statements_executed, select_statements, retval = \
            _run_statements_sqlite(db_path, stmts, sqlite_pragmas)
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
