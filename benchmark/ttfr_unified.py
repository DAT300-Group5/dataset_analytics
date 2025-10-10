#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified TTFR measurer for SQLite, DuckDB, and chDB.

Usage:
  python ttfr_unified.py <engine> <db_path> <sql_file> [--batch-size N]

Where:
  <engine> in {sqlite, duckdb, chdb}
  --batch-size applies to vectorized engines (duckdb, chdb) only; default 2048.
  For SQLite, TTFR is defined as time to first *row* regardless of --batch-size.

Policy (consistent with the original single-engine scripts):
  - Treat the SQL file as one task. Measure TTFR from *before any statement runs*
    (including preamble DDL/DML/PRAGMAs/settings) until the first output of the *last SELECT*.
  - Also report total time from script start until all rows of the last SELECT are consumed.
  - Only rows from the final SELECT are fetched/consumed and counted.
  - For DuckDB/chDB we fetch in fixed-size batches to match vectorized engines' natural unit.
  - For SQLite we measure time to first row and then drain the cursor.

Notes:
  - Requires a helper: from utils import extract_last_select
    which must return (preamble_stmts: List[str], final_select: str).
"""

from __future__ import annotations

import sys
import time
import argparse

from utils import extract_last_select


def _run_sqlite(db_path: str, sql_file: str) -> None:
    """Measure TTFR on SQLite: time to first *row* for the last SELECT."""
    import sqlite3

    preamble_stmts, final_select = extract_last_select(sql_file)

    conn = sqlite3.connect(db_path)
    conn.row_factory = None
    cur = conn.cursor()

    try:
        t_script_start = time.perf_counter_ns()

        # Execute preamble in a single transaction to avoid per-statement commit cost.
        conn.execute("BEGIN")
        for stmt in preamble_stmts:
            s = stmt.strip()
            if s:
                cur.execute(s)
        conn.execute("COMMIT")

        # Execute the final SELECT and fetch first row (row-level TTFR).
        cur.execute(final_select)
        first_row = cur.fetchone()
        t_after_first = time.perf_counter_ns()

        # Drain remaining rows.
        rows = 0
        if first_row is None:
            rows = 0
        else:
            rows = 1
            for _ in cur:
                rows += 1

        t_after_all = time.perf_counter_ns()

        # Metrics from script start (ms)
        ttfr_ms = (t_after_first - t_script_start) / 1_000_000.0
        total_ms = (t_after_all - t_script_start) / 1_000_000.0

        # Output
        print(f"# Engine: sqlite3")
        print(f"# DB: {db_path}")
        print(f"# SQL file: {sql_file}")
        print(f"# Mode: first-row")
        print(f"# Rows returned: {rows}")
        print(f"# TTFR (ms): {ttfr_ms:.3f}")
        print(f"# Total time (ms): {total_ms:.3f}")

    finally:
        try:
            cur.close()
        finally:
            conn.close()


def _run_duckdb(db_path: str, sql_file: str, batch_size: int) -> None:
    """Measure TTFR on DuckDB: time to first *batch* for the last SELECT."""
    import duckdb

    preamble_stmts, final_select = extract_last_select(sql_file)
    con = duckdb.connect(database=db_path)

    try:
        t_script_start = time.perf_counter_ns()

        con.execute("BEGIN")
        for stmt in preamble_stmts:
            s = stmt.strip()
            if s:
                con.execute(s)
        con.execute("COMMIT")

        con.execute(final_select)

        first_batch = con.fetchmany(batch_size)
        t_after_first = time.perf_counter_ns()

        first_rows = len(first_batch)
        total_rows = first_rows

        while True:
            batch = con.fetchmany(batch_size)
            if not batch:
                break
            total_rows += len(batch)

        t_after_all = time.perf_counter_ns()

        ttfr_ms = (t_after_first - t_script_start) / 1_000_000.0
        total_ms = (t_after_all - t_script_start) / 1_000_000.0

        print(f"# Engine: duckdb")
        print(f"# DB: {db_path}")
        print(f"# SQL file: {sql_file}")
        print(f"# Mode: first-batch")
        print(f"# Batch size: {batch_size}")
        print(f"# First-batch rows: {first_rows}")
        print(f"# Total rows: {total_rows}")
        print(f"# TTFR (ms): {ttfr_ms:.3f}")
        print(f"# Total time (ms): {total_ms:.3f}")

    finally:
        con.close()


def _run_chdb(db_path: str, sql_file: str, batch_size: int) -> None:
    """Measure TTFR on chDB: time to first *batch* for the last SELECT."""
    import chdb

    preamble_stmts, final_select = extract_last_select(sql_file)

    conn = chdb.connect(db_path)
    cur = conn.cursor()

    try:
        t_script_start = time.perf_counter_ns()

        # chDB/ClickHouse style: execute preamble statements one by one.
        for stmt in preamble_stmts:
            s = stmt.strip()
            if s:
                cur.execute(s)

        cur.execute(final_select)

        first_batch = cur.fetchmany(batch_size)
        t_after_first = time.perf_counter_ns()

        first_rows = len(first_batch)
        total_rows = first_rows

        while True:
            batch = cur.fetchmany(batch_size)
            if not batch:
                break
            total_rows += len(batch)

        t_after_all = time.perf_counter_ns()

        ttfr_ms = (t_after_first - t_script_start) / 1_000_000.0
        total_ms = (t_after_all - t_script_start) / 1_000_000.0

        print(f"# Engine: chdb")
        print(f"# DB: {db_path}")
        print(f"# SQL file: {sql_file}")
        print(f"# Mode: first-batch")
        print(f"# Batch size: {batch_size}")
        print(f"# First-batch rows: {first_rows}")
        print(f"# Total rows: {total_rows}")
        print(f"# TTFR (ms): {ttfr_ms:.3f}")
        print(f"# Total time (ms): {total_ms:.3f}")

    finally:
        try:
            cur.close()
        finally:
            conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure TTFR for the last SELECT across engines.")
    parser.add_argument("engine", choices=["sqlite", "duckdb", "chdb"], help="Which engine to use.")
    parser.add_argument("db_path", help="Path to the database file (or :memory: where supported).")
    parser.add_argument("sql_file", help="Path to the SQL script.")
    parser.add_argument("--batch-size", type=int, default=2048,
                        help="Batch size for vectorized engines (duckdb, chdb). Ignored by sqlite.")
    args = parser.parse_args()

    # Dispatch without importing all engines up front.
    if args.engine == "sqlite":
        try:
            _run_sqlite(args.db_path, args.sql_file)
        except Exception as e:
            print(f"SQLite error: {e}")
            sys.exit(1)
    elif args.engine == "duckdb":
        try:
            _run_duckdb(args.db_path, args.sql_file, args.batch_size)
        except Exception as e:
            print(f"DuckDB error: {e}")
            sys.exit(1)
    else:  # chdb
        try:
            _run_chdb(args.db_path, args.sql_file, args.batch_size)
        except Exception as e:
            print(f"chDB error: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
