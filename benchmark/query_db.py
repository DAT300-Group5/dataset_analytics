
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
query_db.py — Execute a semicolon-separated SQL string on SQLite / DuckDB / chDB,
and report TTFR for the **LAST SELECT** with unified semantics.

API (simplified):

        run_query_with_ttfr(engine: str, db_path: str, query: str, threads: int | None = None) -> dict

Return keys:
    - retval            # "OK" or rows_returned for the last SELECT
    - ttfr_seconds      # TTFR of the LAST SELECT from script start to first output
    - rows_returned     # total rows of the LAST SELECT

Engine-specific TTFR unit:
    - SQLite: first row
    - DuckDB/chDB: first batch (fixed BATCH_SIZE to reflect vectorized execution)
"""

from __future__ import annotations

import time
from typing import List, Optional, Tuple

from utils import extract_last_select

# Fixed batch for vectorized engines (align with ttfr_unified policy)
BATCH_SIZE = 2048


def _sqlite_run(db_path: str, preamble: List[str], final_select: Optional[str]) -> Tuple[Optional[float], int, str]:
    import sqlite3

    rows_returned = 0
    ttfr: Optional[float] = None
    retval: str | int = "OK"

    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        t0 = time.perf_counter()  # script start

        # Execute preamble inside a transaction to avoid per-statement commit cost.
        con.execute("BEGIN")
        for s in preamble:
            ss = s.strip()
            if ss:
                cur.execute(ss)
        con.execute("COMMIT")

        if final_select is None:
            ttfr = None
            rows_returned = 0
            retval = "OK"
        else:
            cur.execute(final_select)
            first_row = cur.fetchone()
            ttfr = time.perf_counter() - t0

            if first_row is None:
                rows_returned = 0
                retval = 0
            else:
                rows_returned = 1
                for _ in cur:
                    rows_returned += 1
                retval = rows_returned

        return ttfr, rows_returned, retval
    finally:
        con.close()


def _duckdb_run(db_path: str, preamble: List[str], final_select: Optional[str], threads: Optional[int]) -> Tuple[Optional[float], int, str]:
    import duckdb

    rows_returned = 0
    ttfr: Optional[float] = None
    retval: str | int = "OK"

    config = {}
    if threads is not None and threads > 0:
        config["threads"] = int(threads)

    con = duckdb.connect(database=db_path, config=config or None)
    try:
        t0 = time.perf_counter()  # script start

        con.execute("BEGIN")
        for s in preamble:
            ss = s.strip()
            if ss:
                con.execute(ss)
        con.execute("COMMIT")

        if final_select is None:
            ttfr = None
            rows_returned = 0
            retval = "OK"
        else:
            con.execute(final_select)
            first_batch = con.fetchmany(BATCH_SIZE)
            ttfr = time.perf_counter() - t0

            first_rows = len(first_batch)
            rows_returned = first_rows
            while True:
                batch = con.fetchmany(BATCH_SIZE)
                if not batch:
                    break
                rows_returned += len(batch)

            retval = rows_returned

        return ttfr, rows_returned, retval
    finally:
        con.close()


def _chdb_run(db_path: str, preamble: List[str], final_select: Optional[str], threads: Optional[int]) -> Tuple[Optional[float], int, str]:
    import chdb

    rows_returned = 0
    ttfr: Optional[float] = None
    retval: str | int = "OK"

    conn = chdb.connect(db_path)
    cur = conn.cursor()
    try:
        if threads and int(threads) > 0:
            cur.execute(f"SET max_threads = {int(threads)}")

        t0 = time.perf_counter()  # script start

        for s in preamble:
            ss = s.strip()
            if ss:
                cur.execute(ss)

        if final_select is None:
            ttfr = None
            rows_returned = 0
            retval = "OK"
        else:
            cur.execute(final_select)
            first_batch = cur.fetchmany(BATCH_SIZE)
            ttfr = time.perf_counter() - t0

            first_rows = len(first_batch)
            rows_returned = first_rows
            while True:
                batch = cur.fetchmany(BATCH_SIZE)
                if not batch:
                    break
                rows_returned += len(batch)

            retval = rows_returned

        return ttfr, rows_returned, retval
    finally:
        try:
            cur.close()
        finally:
            conn.close()


# ------------------------------ Public API --------------------------------- #

def run_query_with_ttfr(engine: str, db_path: Optional[str], query: str,
                        threads: Optional[int] = None) -> dict:
    """
    Execute a semicolon-separated SQL string as one task and report metrics.
    API/return keys are preserved for compatibility.

    Parameters:
      engine: "duckdb" | "sqlite" | "chdb"
      db_path: path to database
      query: semicolon-separated SQL statements
      threads: optional engine-specific parallelism (DuckDB: PRAGMA threads, chdb: max_threads)

    Returns:
      dict with keys:
        - retval: "OK" or rows_returned for the last SELECT
        - ttfr_seconds: TTFR of the LAST SELECT from *script start* to first output
        - rows_returned: total rows of the LAST SELECT
    """
    if not db_path:
        raise ValueError("db_path must be provided")
    
    preamble, final_select = extract_last_select(query)

    if engine == "duckdb":
        ttfr, rows_returned, retval = _duckdb_run(db_path, preamble, final_select, threads)
    elif engine == "sqlite":
        ttfr, rows_returned, retval = _sqlite_run(db_path, preamble, final_select)
    elif engine == "chdb":
        ttfr, rows_returned, retval = _chdb_run(db_path, preamble, final_select, threads)
    else:
        raise ValueError(f"Unsupported engine: {engine}")

    return {
        "retval": retval,
        "ttfr_seconds": ttfr,
        "rows_returned": rows_returned
    }
