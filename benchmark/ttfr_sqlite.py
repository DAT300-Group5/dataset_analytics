#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Measure TTFR (Time To First Row) for the *last* SELECT in a SQL script on SQLite.

Usage:
  python ttfr_sqlite.py /path/to/db.sqlite /path/to/query.sql

Policy:
  - Treat the SQL file as one task. TTFR is measured from *before any statement runs*
    (including preamble DDL/DML/PRAGMAs) until the first row of the *last SELECT*.
  - Also report total time from script start until all rows of the last SELECT are consumed.
  - Only rows from the final SELECT are fetched/consumed and counted.
"""

import sys
import sqlite3
import time

from utils import extract_last_select


def measure_ttfr_script_start(db_path: str, sql_file: str) -> None:
    # Parse SQL file and locate the last SELECT
    preamble_stmts, final_select = extract_last_select(sql_file)

    conn = sqlite3.connect(db_path)
    conn.row_factory = None
    cur = conn.cursor()

    try:
        # Start timing *before any statement runs*
        t_script_start = time.perf_counter_ns()

        # Execute preamble one-by-one (no result fetching even if a preamble stmt returns rows)
        # before the preamble loop
        conn.execute("BEGIN")
        for stmt in preamble_stmts:
            if stmt.strip():
                cur.execute(stmt)
        conn.execute("COMMIT")

        # Execute the final SELECT and measure time to the first row (TTFR)
        cur.execute(final_select)
        first_row = cur.fetchone()
        t_after_first_row = time.perf_counter_ns()

        # Consume remaining rows without fetchall()
        rows = 0
        if first_row is None:
            rows = 0
        else:
            rows = 1
            for _ in cur:
                rows += 1

        t_after_all_rows = time.perf_counter_ns()

        # Metrics from *script start* (milliseconds)
        ttfr_ms = (t_after_first_row - t_script_start) / 1_000_000.0
        total_ms = (t_after_all_rows - t_script_start) / 1_000_000.0

        # Output
        print(f"# DB: {db_path}")
        print(f"# SQL file: {sql_file}")
        print(f"# Rows returned: {rows}")
        print(f"# TTFR (ms): {ttfr_ms:.3f}")
        print(f"# Total time (ms): {total_ms:.3f}")

    finally:
        try:
            cur.close()
        finally:
            conn.close()


def main():
    if len(sys.argv) != 3:
        print("Usage: python ttfr_sqlite.py /path/to/db.sqlite /path/to/query.sql")
        sys.exit(2)
    db_path, sql_file = sys.argv[1], sys.argv[2]
    try:
        measure_ttfr_script_start(db_path, sql_file)
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
