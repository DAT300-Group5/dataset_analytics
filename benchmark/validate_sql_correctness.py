
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Result Equivalence Validator

This module provides functionality to validate SQL query result equivalence across
different database engines. It compares query results from multiple database engines
to ensure they produce equivalent outputs.

Features:
- Supports multiple database engines: DuckDB, SQLite, and ClickHouse (via chdb)
- Flexible comparison modes: ordered and bag (multiset) comparison
- Configurable floating-point tolerance for numeric comparisons
- Detailed mismatch reporting with sample data
- JSON and human-readable output formats

Usage:
python validate_sql_correctness.py \
    --case duckdb ./db_vs14/vs14_data.duckdb queries/Q1/Q1_duckdb.sql \
    --case sqlite ./db_vs14/vs14_data.sqlite queries/Q1/Q1_sqlite.sql \
    --case chdb   ./db_vs14/vs14_data_chdb  queries/Q1/Q1_clickhouse.sql \
    --mode bag --output human --show 5 --json-file q1_diff.json

Example:
    Validate query results across three engines:
        $ python validate_sql_correctness.py \
            --case duckdb db.duckdb query.sql \
            --case sqlite db.sqlite query.sql \
            --mode bag --verbose
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import re
import sqlite3
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import duckdb
except ImportError:
    duckdb = None

# ClickHouse support via chdb is optional
try:
    import chdb as _chdb  # noqa: F401
    HAS_CHDB = True
except ImportError:
    HAS_CHDB = False

# Constants
DEFAULT_BATCH_SIZE = 8192
DEFAULT_FLOAT_TOLERANCE = 1e-9
DEFAULT_DECIMAL_PLACES = 16
DEFAULT_SAMPLE_LIMIT = 5

# Special tokens for value normalization
_NaN_TOKEN = ("<NaN>",)
_POS_INF_TOKEN = "<+INF>"
_NEG_INF_TOKEN = "<-INF>"
_BYTES_TOKEN = "<BYTES>"

# ------------------------------ SQL loading ---------------------------------

def load_sql(path: str) -> str:
    """Load SQL content from a file.
    
    Args:
        path: Path to the SQL file.
        
    Returns:
        The content of the SQL file as a string.
    """
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def split_statements(sql: str) -> list[str]:
    """Split SQL content into individual statements.
    
    Args:
        sql: The SQL content to split.
        
    Returns:
        A list of non-empty SQL statements.
    """
    parts = [s.strip() for s in sql.split(';')]
    return [p for p in parts if p]


def is_select(stmt: str) -> bool:
    """Check if a SQL statement is a SELECT-like query.
    
    Args:
        stmt: The SQL statement to check.
        
    Returns:
        True if the statement is a SELECT, WITH, SHOW, DESCRIBE, or EXPLAIN.
    """
    s = stmt.lstrip().lower()
    return s.startswith(("select", "with", "show", "describe", "explain"))

# --------------------------- Value normalization ----------------------------

def _decimals_from_tol(tol: float) -> int:
    """Calculate decimal places from tolerance value.
    
    Args:
        tol: The floating point tolerance.
        
    Returns:
        Number of decimal places to round to.
    """
    if tol <= 0:
        return DEFAULT_DECIMAL_PLACES
    return max(0, int(math.ceil(-math.log10(tol))))


def normalize_value(v: Any, float_tol: float) -> Any:
    """Normalize a value to a canonical, JSON-safe form.
    
    This function handles engine-specific data types and converts them
    to standardized representations for comparison purposes.
    
    Args:
        v: The value to normalize.
        float_tol: Tolerance for floating point comparisons.
        
    Returns:
        The normalized value in a JSON-serializable format.
    """
    import math as _m

    if v is None:
        return None
    if isinstance(v, float):
        if _m.isnan(v):
            return _NaN_TOKEN
        if _m.isinf(v):
            return (_POS_INF_TOKEN if v > 0 else _NEG_INF_TOKEN)
        return round(v, _decimals_from_tol(float_tol))
    try:
        import decimal
        if isinstance(v, decimal.Decimal):
            q = decimal.Decimal(str(float_tol))
            return str(v.quantize(q, rounding=decimal.ROUND_HALF_EVEN))
    except Exception:
        pass
    if isinstance(v, (bytes, bytearray, memoryview)):
        return (_BYTES_TOKEN, bytes(v).hex())
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            pass
    try:
        import numpy as np
        if isinstance(v, (np.generic,)):
            return v.item()
    except Exception:
        pass
    try:
        json.dumps(v)
        return v
    except TypeError:
        return repr(v)

def normalize_row(row: tuple, float_tol: float) -> tuple:
    """Normalize all values in a row.
    
    Args:
        row: The row tuple to normalize.
        float_tol: Tolerance for floating point comparisons.
        
    Returns:
        A tuple with all values normalized.
    """
    return tuple(normalize_value(v, float_tol) for v in row)


def rows_to_bag(rows: List[Tuple], float_tol: float):
    """Convert a list of rows to a bag (multiset) representation.
    
    Args:
        rows: List of row tuples.
        float_tol: Tolerance for floating point comparisons.
        
    Returns:
        A collections.Counter object representing the multiset of rows.
    """
    import collections
    normd = [normalize_row(r, float_tol) for r in rows]
    return collections.Counter(normd)

# ------------------------------ Runners -------------------------------------

def _fetch_all(cur: Any, arraysize: int = DEFAULT_BATCH_SIZE) -> List[Tuple]:
    """Fetch all rows from a database cursor in batches.
    
    Args:
        cur: Database cursor.
        arraysize: Size of each batch to fetch.
        
    Returns:
        List of all rows as tuples.
    """
    rows = []
    try:
        cur.arraysize = arraysize
    except Exception:
        pass
    while True:
        batch = cur.fetchmany()
        if not batch:
            break
        rows.extend(batch)
    return rows


def run_sqlite(db_path: str, query: str) -> Tuple[List[str], List[Tuple]]:
    """Execute SQL query on SQLite database.
    
    Args:
        db_path: Path to SQLite database file.
        query: SQL query to execute.
        
    Returns:
        Tuple of (column_names, rows) where column_names is a list of strings
        and rows is a list of tuples containing the query results.
    """
    con = sqlite3.connect(db_path)
    try:
        con.isolation_level = None
        cur = con.cursor()
        rows_acc = []
        columns = None
        for stmt in split_statements(query):
            if is_select(stmt):
                cur.execute(stmt)
                if columns is None:
                    columns = [d[0] for d in (cur.description or [])]
                rows_acc = _fetch_all(cur)
            else:
                cur.execute(stmt)
        return (columns or []), rows_acc
    finally:
        con.close()

def run_duckdb(db_path: str, query: str, threads: Optional[int] = None) -> Tuple[List[str], List[Tuple]]:
    """Execute SQL query on DuckDB database.
    
    Args:
        db_path: Path to DuckDB database file.
        query: SQL query to execute.
        threads: Number of threads to use (optional).
        
    Returns:
        Tuple of (column_names, rows) where column_names is a list of strings
        and rows is a list of tuples containing the query results.
    """
    if duckdb is None:
        raise ImportError("DuckDB is not available")
    
    con = duckdb.connect(database=db_path)
    try:
        if threads and threads > 0:
            try:
                con.execute(f"PRAGMA threads={int(threads)}")
            except Exception:
                pass
        rows_acc = []
        columns = None
        for stmt in split_statements(query):
            if is_select(stmt):
                cur = con.execute(stmt)
                if columns is None:
                    columns = [d[0] for d in (cur.description or [])]
                rows_acc = _fetch_all(cur)
            else:
                con.execute(stmt)
        return (columns or []), rows_acc
    finally:
        con.close()


def run_chdb(db_path: str, query: str, threads: Optional[int] = None) -> Tuple[List[str], List[Tuple]]:
    """Execute SQL query on ClickHouse database via chdb.
    
    Args:
        db_path: Path to ClickHouse database directory or connection string.
        query: SQL query to execute.
        threads: Number of threads to use (optional).
        
    Returns:
        Tuple of (column_names, rows) where column_names is a list of strings
        and rows is a list of tuples containing the query results.
    """
    import chdb
    conn = chdb.connect(db_path if db_path else ":memory:")
    try:
        cur = conn.cursor()
        if threads and int(threads) > 0:
            cur.execute(f"SET max_threads = {int(threads)}")
        rows_acc = []
        columns = None
        for stmt in split_statements(query):
            cur.execute(stmt)
            if is_select(stmt):
                if columns is None and cur.description:
                    columns = [d[0] for d in cur.description]
                rows_acc = _fetch_all(cur)
        cur.close()
        return (columns or []), rows_acc
    finally:
        try:
            conn.close()
        except Exception:
            pass

RUNNERS = {
    "duckdb": run_duckdb,
    "sqlite": run_sqlite,
    "chdb": run_chdb,
}

# ------------------------------ Comparison ----------------------------------

def compare_results(
    a_cols: List[str], 
    a_rows: List[Tuple], 
    b_cols: List[str], 
    b_rows: List[Tuple], 
    mode: str, 
    float_tol: float, 
    ignore_colnames: bool, 
    sample_limit: int = 10
) -> Dict[str, Any]:
    """Compare query results from two different engines.
    
    Args:
        a_cols: Column names from first result set.
        a_rows: Row data from first result set.
        b_cols: Column names from second result set.
        b_rows: Row data from second result set.
        mode: Comparison mode ('ordered' or 'bag').
        float_tol: Tolerance for floating point comparisons.
        ignore_colnames: Whether to ignore column name differences.
        sample_limit: Maximum number of sample differences to report.
        
    Returns:
        Dictionary containing comparison results with 'ok' boolean and
        detailed information about any differences found.
    """
    result = {
        "ok": True,
        "column_issue": None,  # {"a_cols": [...], "b_cols": [...], "hints": [...]}
        "row_issue": None,     # {"mode": "ordered"/"bag", "detail": {...}}
    }

    # Columns
    def names_norm(cols):
        return [str(c or "").strip().lower() for c in cols]

    col_hints = []
    if len(a_cols) != len(b_cols) or (not ignore_colnames and names_norm(a_cols) != names_norm(b_cols)):
        # Hints: strip table prefixes, alias detection
        def strip_prefix(c): return str(c).split('.', 1)[-1]
        if [strip_prefix(x) for x in a_cols] == [strip_prefix(x) for x in b_cols]:
            col_hints.append("Column name prefixes differ (e.g., 'm.'). Consider aliasing without table prefixes.")
        alias_pairs = {("total_steps","steps_sum"), ("steps_sum","total_steps")}
        for ac, bc in zip(a_cols, b_cols):
            if (ac, bc) in alias_pairs:
                col_hints.append(f"Possible alias: '{ac}' ~ '{bc}'. Consider renaming to match.")
                break

        result["ok"] = False
        result["column_issue"] = {"a_cols": a_cols, "b_cols": b_cols, "hints": col_hints}

    # Rows
    if mode == "ordered":
        if len(a_rows) != len(b_rows):
            result["ok"] = False
            result["row_issue"] = {"mode": "ordered", "detail": {"type": "row_count_mismatch", "a_count": len(a_rows), "b_count": len(b_rows)}}
        else:
            def row_repr_seq(rows):
                return [json.dumps(normalize_row(r, float_tol), ensure_ascii=False, sort_keys=True) for r in rows]
            a_seq = row_repr_seq(a_rows)
            b_seq = row_repr_seq(b_rows)
            if a_seq != b_seq:
                result["ok"] = False
                diffs = []
                for i, (ra, rb) in enumerate(itertools.zip_longest(a_seq, b_seq, fillvalue="<NO ROW>")):
                    if ra != rb:
                        diffs.append({"index": i, "a": ra, "b": rb})
                    if len(diffs) >= sample_limit:
                        break
                result["row_issue"] = {"mode": "ordered", "detail": {"type": "ordered_diff", "samples": diffs}}
    elif mode in ("bag", "multiset"):
        from collections import Counter
        a_bag = rows_to_bag(a_rows, float_tol)
        b_bag = rows_to_bag(b_rows, float_tol)
        if a_bag != b_bag:
            result["ok"] = False
            missing = []
            extra = []
            for k in (a_bag.keys() | b_bag.keys()):
                ca = a_bag.get(k, 0)
                cb = b_bag.get(k, 0)
                if ca > cb:
                    missing.append({"row": k, "count_diff": ca - cb})
                elif cb > ca:
                    extra.append({"row": k, "count_diff": cb - ca})
            result["row_issue"] = {
                "mode": "bag",
                "detail": {
                    "type": "multiset_diff",
                    "missing_count": len(missing),
                    "extra_count": len(extra),
                    "missing_samples": missing[:sample_limit],
                    "extra_samples": extra[:sample_limit],
                },
            }
    else:
        raise ValueError(f"Unknown mode: {mode}")

    return result

# ------------------------------ Main CLI ------------------------------------

def main() -> None:
    """Main function to handle command line interface and coordinate validation."""
    ap = argparse.ArgumentParser(
        description="Result equivalence checker across explicit ENGINE-DB-SQL cases"
    )
    ap.add_argument(
        "--case", nargs=3, action="append",
        metavar=("ENGINE", "DB_PATH", "SQL_FILE"),
        help="One case: ENGINE DB_PATH SQL_FILE. Repeat for multiple cases."
    )
    ap.add_argument(
        "--threads", type=int, default=0, 
        help="DuckDB/chDB threads"
    )
    ap.add_argument(
        "--mode", choices=["ordered", "bag"], default="bag", 
        help="Equivalence model"
    )
    ap.add_argument(
        "--float-tol", type=float, default=DEFAULT_FLOAT_TOLERANCE, 
        help="Floating point tolerance (rounding granularity)"
    )
    ap.add_argument(
        "--ignore-colnames", action="store_true", 
        help="Ignore column name differences; only check column count"
    )
    ap.add_argument(
        "--stop-on-first-fail", action="store_true", 
        help="Stop at the first mismatch"
    )
    ap.add_argument(
        "--verbose", action="store_true", 
        help="Verbose execution logs"
    )
    ap.add_argument(
        "--output", choices=["human", "json"], default="human", 
        help="Output format"
    )
    ap.add_argument(
        "--show", type=int, default=DEFAULT_SAMPLE_LIMIT, 
        help="Max number of sample row differences to display"
    )
    ap.add_argument(
        "--json-file", type=str, default="", 
        help="Also write full JSON summary to this path"
    )
    args = ap.parse_args()

    if not args.case or len(args.case) < 2:
        print(
            "Error: Provide at least two --case entries (ENGINE DB_PATH SQL_FILE).", 
            file=sys.stderr
        )
        sys.exit(2)

    # Validate and run each case
    per_case = []
    for (engine, dbp, sqlf) in args.case:
        eng = engine.strip().lower()
        
        # Validate engine type
        if eng not in RUNNERS:
            print(
                f"Unknown engine '{engine}'. Must be one of: {list(RUNNERS)}", 
                file=sys.stderr
            )
            sys.exit(2)
            
        # Check for optional engine availability
        if eng == "chdb" and not HAS_CHDB:
            print("Error: chdb is not installed but requested.", file=sys.stderr)
            sys.exit(2)
            
        if eng == "duckdb" and duckdb is None:
            print("Error: duckdb is not installed but requested.", file=sys.stderr)
            sys.exit(2)
            
        # Validate file paths
        if not os.path.exists(dbp):
            print(f"Database file not found: {dbp}", file=sys.stderr)
            sys.exit(2)
        if not os.path.exists(sqlf):
            print(f"SQL file not found: {sqlf}", file=sys.stderr)
            sys.exit(2)

        try:
            sql = load_sql(sqlf)
            runner = RUNNERS[eng]
            t0 = time.perf_counter()
            
            # Execute query with appropriate parameters
            if eng in ("duckdb", "chdb"):
                cols, rows = runner(dbp, sql, threads=args.threads)
            else:
                cols, rows = runner(dbp, sql)
            t1 = time.perf_counter()
            
            per_case.append({
                "label": f'{eng}:{dbp}|{os.path.basename(sqlf)}',
                "engine": eng,
                "db": dbp,
                "sql_file": sqlf,
                "columns": cols,
                "row_count": len(rows),
                "rows": rows,
                "time_sec": t1 - t0,
            })
            
            if args.verbose:
                print(
                    f'[{eng}] rows={len(rows)} cols={len(cols)} '
                    f'time={t1-t0:.3f}s on {sqlf}'
                )
        except Exception as e:
            print(f"Error executing {eng} query: {e}", file=sys.stderr)
            sys.exit(2)

    # Pairwise comparisons between all cases
    any_fail = False
    comparisons = {}
    
    for i in range(len(per_case)):
        for j in range(i + 1, len(per_case)):
            a = per_case[i]
            b = per_case[j]
            
            # Compare results between engines
            comp = compare_results(
                a["columns"], a["rows"], 
                b["columns"], b["rows"],
                mode=args.mode, 
                float_tol=args.float_tol,
                ignore_colnames=args.ignore_colnames, 
                sample_limit=args.show
            )
            
            key = f'{a["label"]} ~ {b["label"]}'
            comparisons[key] = comp
            
            if not comp["ok"]:
                any_fail = True
                if args.stop_on_first_fail:
                    break
                    
        if args.stop_on_first_fail and any_fail:
            break

    # Build summary report (omit row data for compactness)
    summary = {
        "mode": args.mode,
        "float_tol": args.float_tol,
        "ignore_colnames": args.ignore_colnames,
        "cases": [
            {k: v for k, v in pc.items() if k != "rows"} 
            for pc in per_case
        ],
        "comparisons": comparisons,
    }

    # Write JSON output file if requested
    if args.json_file:
        try:
            with open(args.json_file, "w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            if args.verbose:
                print(f"JSON summary written to: {args.json_file}")
        except Exception as e:
            print(f"Warning: Failed to write JSON file '{args.json_file}': {e}", file=sys.stderr)

    # Output
    if args.output == "json":
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print("\n=== Result Equivalence Report (mode=%s, tol=%g) ===" % (args.mode, args.float_tol))
        for key, comp in comparisons.items():
            status = "OK" if comp["ok"] else "FAIL"
            print(f"\n--- {key} : {status} ---")
            if comp["column_issue"]:
                print("Columns differ:")
                print("  A:", comp["column_issue"]["a_cols"])
                print("  B:", comp["column_issue"]["b_cols"])
                for h in comp["column_issue"]["hints"] or []:
                    print("  hint:", h)
            if comp["row_issue"]:
                d = comp["row_issue"]["detail"]
                if d["type"] == "row_count_mismatch":
                    print(f"Rows differ: count {d['a_count']} vs {d['b_count']}")
                elif d["type"] == "ordered_diff":
                    print("Rows differ (ordered). Sample diffs:")
                    for s in d["samples"]:
                        print(f"  idx={s['index']}")
                        print(f"    A: {s['a']}")
                        print(f"    B: {s['b']}")
                elif d["type"] == "multiset_diff":
                    print(f"Rows differ (bag). Missing kinds: {d['missing_count']}, Extra kinds: {d['extra_count']}")
                    if d["missing_samples"]:
                        print("  Missing samples (A > B):")
                        for s in d["missing_samples"]:
                            print(f"    {s['row']} x{s['count_diff']}")
                    if d["extra_samples"]:
                        print("  Extra samples (B > A):")
                        for s in d["extra_samples"]:
                            print(f"    {s['row']} x{s['count_diff']}")
        print("\n=== End ===")

    sys.exit(1 if any_fail else 0)


if __name__ == "__main__":
    main()
