#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run SQL file against SQLite / DuckDB / chDB and print the final result-set as CSV.

Behavior:
- Executes statements in order (preamble first).
- Tracks the *last* statement that returns a result-set (typically the last SELECT).
- Prints that result-set (header + rows) to stdout as CSV.

Usage:
  python run_sql_any.py DB_PATH SQL_FILE [--engine {sqlite,duckdb,chdb}]

Engine inference (if --engine omitted):
- .sqlite, .db  -> sqlite
- .duckdb       -> duckdb
- otherwise     -> sqlite (fallback)
"""

import sys
import csv
import argparse

from utils import load_query_from_file, split_statements


def run_sqlite(db_path: str, sql: str):
    import sqlite3
    con = sqlite3.connect(db_path)
    # match your original: autocommit-style and explicit cursor reuse
    con.isolation_level = None
    cur = con.cursor()
    cols, rows = None, []

    for stmt in split_statements(sql):
        cur.execute(stmt)
        if cur.description:  # a result-set returning statement
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

    cur.close()
    con.close()
    return cols, rows


def run_duckdb(db_path: str, sql: str):
    import duckdb
    con = duckdb.connect(db_path)
    cols, rows = None, []

    for stmt in split_statements(sql):
        cur = con.execute(stmt)  # duckdb returns a cursor-like object
        if cur.description:
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

    cur.close()
    con.close()
    return cols, rows


def run_chdb(db_path: str, sql: str):
    import chdb
    con = chdb.connect(db_path)
    cur = con.cursor()
    cols, rows = None, []

    for stmt in split_statements(sql):
        cur.execute(stmt)
        if cur.description:
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

    cur.close()
    con.close()
    return cols, rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("db_path")
    ap.add_argument("sql_file")
    ap.add_argument("--engine", choices=["sqlite", "duckdb", "chdb"], help="Override engine detection")
    args = ap.parse_args()

    engine = args.engine
    sql = load_query_from_file(args.sql_file)

    if engine == "sqlite":
        cols, rows = run_sqlite(args.db_path, sql)
    elif engine == "duckdb":
        cols, rows = run_duckdb(args.db_path, sql)
    else:
        cols, rows = run_chdb(args.db_path, sql)

    if cols:
        w = csv.writer(sys.stdout)
        w.writerow(cols)
        w.writerows(rows)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print(__doc__.strip())
        sys.exit(1)
    main()
