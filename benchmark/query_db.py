#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
query_db.py — Execute SQL queries with SQLite / DuckDB / chDB,
measuring TTFR (time-to-first-result), row counts, etc.

Notes on TTFR:
- For vectorized/columnar engines (DuckDB/chDB), "first result" is effectively
  the first underlying data block becoming available, even if we fetch only 1 row
  at the Python layer. So the metric approximates "time-to-first-block".
- For SQLite (row-by-row iteration), TTFR is closer to a true "first row" time.
"""

from __future__ import annotations

import sqlite3
import time
from typing import Tuple, List, Optional

try:
    import duckdb  # type: ignore
except ImportError:
    duckdb = None


def _is_select_statement(stmt: str) -> bool:
    """Heuristically detect if a statement is SELECT-like."""
    s = stmt.lstrip().lower()
    return s.startswith(("select", "with", "show", "describe", "explain"))


def _split_semicolon_statements(sql_text: str) -> List[str]:
    """
    Split a semicolon-separated SQL text into individual statements.
    Trims whitespace and drops empty fragments.
    """
    return [s.strip() for s in sql_text.split(";") if s.strip()]


def _execute_select_ttfr(cursor_or_conn, stmt: str, arraysize: int = 1) -> Tuple[float, int, int]:
    """
    Execute a SELECT and measure TTFR with the following semantics:
      - Start timing just before .execute(stmt)
      - Perform a first fetch with fetchmany(arraysize) to obtain the "first result"
      - Report TTFR as time from execute() start to the completion of that first fetch
      - Drain the remainder to count total rows

    Returns:
      (ttfr_seconds, first_batch_rows, total_rows)

    IMPORTANT:
      For vectorized/columnar engines (DuckDB/chDB) the driver typically pulls a
      whole data block under the hood, so this approximates "time-to-first-block".
    """
    t0 = time.perf_counter()

    # Normalize to a cursor that supports fetchmany
    if hasattr(cursor_or_conn, "fetchmany"):  # SQLite cursor
        cur = cursor_or_conn
        cur.execute(stmt)
    else:  # DuckDB connection: .execute returns a cursor-like object
        cur = cursor_or_conn.execute(stmt)

    # First fetch (bias to 1 row to limit client-side overhead)
    first_batch = cur.fetchmany(arraysize)
    ttfr = time.perf_counter() - t0

    first_rows = len(first_batch)
    total_rows = first_rows

    # Drain remaining batches to count total rows
    while True:
        batch = cur.fetchmany()
        if not batch:
            break
        total_rows += len(batch)

    return ttfr, first_rows, total_rows


def _execute_non_select_statement(cursor_or_conn, stmt: str) -> str:
    """
    Execute a non-SELECT statement for SQLite cursor or DuckDB connection/cursor.
    Returns "OK" when no exception is raised.
    """
    if hasattr(cursor_or_conn, "execute"):
        cursor_or_conn.execute(stmt)
        return "OK"
    raise RuntimeError("Unsupported connection type for non-SELECT execution")


# ----------------------------- Engine Runners ------------------------------ #

def _run_statements_duckdb(
    db_path: str,
    statements: List[str],
    threads: Optional[int] = None
):
    """
    Execute semicolon-split statements on DuckDB.

    Returns:
      tuple: (first_select_ttfr, rows_returned, statements_executed, select_statements, retval)
        - first_select_ttfr: TTFR of the FIRST SELECT encountered (seconds, or None if no SELECT)
        - rows_returned: total rows returned by the LAST SELECT (0 if no SELECT)
        - statements_executed: number of statements executed
        - select_statements: number of SELECT-like statements executed
        - retval: "OK" for non-SELECT, or rows_returned for the last SELECT
    """
    if duckdb is None:
        raise ImportError("duckdb is not installed")

    rows_returned = 0
    first_select_ttfr: Optional[float] = None
    statements_executed = 0
    select_statements = 0
    retval = None

    # Build connection config only when needed (older DuckDB versions may not accept None)
    config: dict = {}
    if threads is not None and threads > 0:
        config["threads"] = int(threads)

    con = duckdb.connect(database=db_path, config=config or None)
    try:
        for stmt in statements:
            statements_executed += 1
            if _is_select_statement(stmt):
                select_statements += 1
                ttfr, _first_rows, total = _execute_select_ttfr(con, stmt, arraysize=1)
                if first_select_ttfr is None:
                    first_select_ttfr = ttfr
                rows_returned = total
                retval = rows_returned
            else:
                retval = _execute_non_select_statement(con, stmt)
    finally:
        con.close()

    return first_select_ttfr, rows_returned, statements_executed, select_statements, retval


def _run_statements_sqlite(db_path: str, statements: List[str]):
    """
    Execute semicolon-split statements on SQLite.

    Returns:
      tuple: (first_select_ttfr, rows_returned, statements_executed, select_statements, retval)
    """
    rows_returned = 0
    first_select_ttfr: Optional[float] = None
    statements_executed = 0
    select_statements = 0
    retval = None

    con = sqlite3.connect(db_path)
    try:
        # autocommit mode
        con.isolation_level = None
        cur = con.cursor()

        for stmt in statements:
            statements_executed += 1
            if _is_select_statement(stmt):
                select_statements += 1
                ttfr, _first_rows, total = _execute_select_ttfr(cur, stmt, arraysize=1)
                if first_select_ttfr is None:
                    first_select_ttfr = ttfr
                rows_returned = total
                retval = rows_returned
            else:
                retval = _execute_non_select_statement(cur, stmt)
    finally:
        con.close()

    return first_select_ttfr, rows_returned, statements_executed, select_statements, retval


def _run_statements_chdb(
    db_path: Optional[str],
    statements: List[str],
    threads: Optional[int] = None
):
    """
    Execute semicolon-split statements on chDB (ClickHouse-in-process).

    Implementation details:
    - Import chdb lazily to avoid importing it in parent processes.
    - Use fetchmany(1) for the first fetch to limit client-side overhead
      and to reduce arraysize-induced bias on TTFR.
    """
    import chdb  # delayed import

    conn = chdb.connect(db_path)
    try:
        cur = conn.cursor()
        if threads and int(threads) > 0:
            cur.execute(f"SET max_threads = {int(threads)}")

        statements_executed = 0
        select_statements = 0
        first_select_ttfr: Optional[float] = None
        rows_returned = 0
        retval = None

        for stmt in statements:
            statements_executed += 1
            if _is_select_statement(stmt):
                select_statements += 1

                # Measure TTFR as execute() -> first fetch(1)
                t0 = time.perf_counter()
                cur.execute(stmt)
                first_batch = cur.fetchmany(1)  # force 1 to reduce arraysize effect
                ttfr = time.perf_counter() - t0

                if first_select_ttfr is None:
                    first_select_ttfr = ttfr

                total = len(first_batch)
                while True:
                    batch = cur.fetchmany()
                    if not batch:
                        break
                    total += len(batch)

                rows_returned = total
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


# ------------------------------ Public API --------------------------------- #

def run_query_with_ttfr(engine: str, db_path: Optional[str], query: str,
                        threads: Optional[int] = None) -> dict:
    """
    Execute a semicolon-separated SQL string on the chosen engine and collect:
      - first_select_ttfr_seconds: TTFR of the FIRST SELECT (None if no SELECT)
      - rows_returned: total rows returned by the LAST SELECT (0 if no SELECT)
      - statements_executed: number of statements executed
      - select_statements: number of SELECT-like statements encountered
      - retval: "OK" or rows_returned for the last SELECT

    TTFR definition in this implementation:
      Time from `execute()` start to completion of the first `fetchmany(1)`.
      For vectorized engines (DuckDB/chDB) this approximates "time-to-first-block".

    Parameters:
      engine: "duckdb" | "sqlite" | "chdb"
      db_path: path to database
      query: semicolon-separated SQL statements
      threads: optional engine-specific parallelism (DuckDB: PRAGMA threads, chdb: max_threads)

    Returns:
      dict with the keys described above.
    """
    if not db_path:
        raise ValueError("db_path must be provided")

    statements = _split_semicolon_statements(query)

    if engine == "duckdb":
        first_ttfr, rows_returned, n_exec, n_select, retval = \
            _run_statements_duckdb(db_path, statements, threads)
    elif engine == "sqlite":
        first_ttfr, rows_returned, n_exec, n_select, retval = \
            _run_statements_sqlite(db_path, statements)
    elif engine == "chdb":
        first_ttfr, rows_returned, n_exec, n_select, retval = \
            _run_statements_chdb(db_path, statements, threads)
    else:
        raise ValueError(f"Unsupported engine: {engine}")

    return {
        "retval": retval,
        "first_select_ttfr_seconds": first_ttfr,
        "rows_returned": rows_returned,
        "statements_executed": n_exec,
        "select_statements": n_select,
    }
