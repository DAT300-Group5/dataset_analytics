#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite TTFR (Time To First Row) minimal script
- Reads ONLY from an .sql file (no -e).
- Executes all statements except the last via executescript().
- Measures TTFR and total time for the LAST statement only.
- Prints ONLY these 3 lines (no CSV / no row data):
    # Rows returned: N
    # TTFR (ms): X.XXX
    # Total time (ms): Y.YYY

Usage:
        python ttfr_sqlite.py DB_PATH FILE.sql
"""

import sys
import os
import re
import time
import sqlite3

def split_sql_text(sql_text: str):
    """
    Robust-ish splitter using sqlite3.complete_statement():
    Accumulate until a statement is syntactically complete.
    """
    stmts = []
    buf = []
    for line in sql_text.splitlines():
        buf.append(line)
        chunk = "\n".join(buf)
        if sqlite3.complete_statement(chunk):
            stmt = chunk.strip()
            if stmt:
                # strip trailing semicolon but keep text
                if stmt.endswith(";"):
                    stmt = stmt[:-1]
                stmts.append(stmt.strip())
            buf = []
    # handle trailing buffer (no semicolon at EOF)
    tail = "\n".join(buf).strip()
    if tail:
        stmts.append(tail)
    if not stmts:
        sys.exit("Error: No SQL statements found in file.")
    return stmts

def first_meaningful_line(sql: str) -> str:
    """
    Try to show a meaningful preview line:
    - skip blanks
    - skip '--' comments
    - skip lines starting with WITH (CTE prolog)
    - prefer a line that contains 'SELECT' if possible
    """
    lines = [ln.rstrip() for ln in sql.splitlines()]
    # 1) Prefer the first line containing SELECT (case-insensitive), not a comment
    for ln in lines:
        s = ln.strip()
        if not s or s.startswith("--"):
            continue
        if re.search(r"\bSELECT\b", s, flags=re.IGNORECASE):
            return s
    # 2) Otherwise, first non-empty, non-comment, non-WITH line
    for ln in lines:
        s = ln.strip()
        if not s or s.startswith("--"):
            continue
        if not s.upper().startswith("WITH"):
            return s
    # 3) Fallback: first non-empty, non-comment line
    for ln in lines:
        s = ln.strip()
        if s and not s.startswith("--"):
            return s
    # 4) Last resort: first line (even if empty)
    return lines[0] if lines else ""

def main():
    if len(sys.argv) != 3:
        sys.exit(f"Usage: {sys.argv[0]} DB_PATH FILE.sql")

    db_path, sql_file = sys.argv[1], sys.argv[2]

    try:
        with open(sql_file, "r", encoding="utf-8") as f:
            sql_text = f.read()
    except OSError as e:
        sys.exit(f"Error reading SQL file: {e}")

    stmts = split_sql_text(sql_text)

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

        # ---- Output: only the requested 3 lines ----
        print(f"# Rows returned: {rows}")
        print(f"# TTFR (ms): {ttfr_ms:.3f}")
        print(f"# Total time (ms): {total_ms:.3f}")

    except sqlite3.Error as e:
        print(f"# [SQLite Error] {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        con.close()

if __name__ == "__main__":
    main()
