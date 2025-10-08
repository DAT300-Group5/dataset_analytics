#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Result Equivalence Validator

This module validates SQL query result equivalence across different database
engines. It executes provided queries against DuckDB, SQLite, and ClickHouse
(via chdb), then compares their outputs using either ordered or multiset
("bag") semantics with configurable floating-point tolerance.

Key capabilities:
- Multiple engines: DuckDB, SQLite, ClickHouse (via chdb)
- Ordered vs. bag comparison
- Stable, JSON-safe normalization for cross-engine types
- Helpful diagnostics for column/row mismatches
- Human-readable or JSON output
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import sys
import time
import decimal
from typing import Any, Dict, List, Optional, Tuple

# Database engines
import sqlite3
import duckdb
import chdb

from utils import load_query_from_file, split_statements, is_select

# ------------------------------- Constants ----------------------------------

DEFAULT_FLOAT_TOLERANCE = 1e-9
DEFAULT_DECIMAL_PLACES = 16
DEFAULT_SAMPLE_LIMIT = 5

# Special tokens for normalization of non-standard values
_NaN_TOKEN = ("<NaN>",)         # tuple on purpose to avoid clashing with real strings
_POS_INF_TOKEN = "<+INF>"
_NEG_INF_TOKEN = "<-INF>"
_BYTES_TOKEN = "<BYTES>"

# --------------------------- Normalization utils ----------------------------

def _decimals_from_tol(tol: float) -> int:
    """
    Convert a floating tolerance (e.g., 1e-9) into number of decimal places.

    tol <= 0 -> use DEFAULT_DECIMAL_PLACES
    """
    if tol <= 0:
        return DEFAULT_DECIMAL_PLACES
    return max(0, int(math.ceil(-math.log10(tol))))


def _decimal_quantize_from_tol(v: decimal.Decimal, tol: float) -> str:
    """
    Quantize a Decimal value to the scale implied by the given tolerance.

    Example: tol=1e-9 -> 9 decimal places -> scale = Decimal('1E-9').
    Returns a string to avoid accidental conversion to binary float in JSON.
    """
    import decimal as _dec
    places = _decimals_from_tol(tol)
    exp = _dec.Decimal(1).scaleb(-places)  # 1E-places
    q = v.quantize(exp, rounding=_dec.ROUND_HALF_EVEN)
    # Use fixed-point representation (no scientific notation) for stable diffs
    return format(q, "f")


def normalize_value(v: Any, float_tol: float) -> Any:
    """
    Normalize a single value into a canonical, JSON-safe representation.

    Goals:
    - Make values comparable across engines/drivers.
    - Preserve enough information to diagnose diffs.
    - Avoid lossy conversions wherever reasonable.

    Normalization rules:
    - None -> None
    - float -> handle NaN/Inf; otherwise round to tolerance-derived decimals
    - Decimal -> quantize to tolerance scale; return as string
    - bytes/bytearray/memoryview -> ("<BYTES>", hexstring)
    - datetime/date/time -> ISO 8601 string via .isoformat()
    - numpy scalars -> .item()
    - other JSON-serializable -> keep as-is
    - fallback -> repr(value)
    """
    import math as _m

    # None
    if v is None:
        return None

    # Python float
    if isinstance(v, float):
        if _m.isnan(v):
            return _NaN_TOKEN
        if _m.isinf(v):
            return _POS_INF_TOKEN if v > 0 else _NEG_INF_TOKEN
        return round(v, _decimals_from_tol(float_tol))

    # Decimal (explicit to avoid falling into isoformat branch)
    try:
        import decimal as _dec
        if isinstance(v, _dec.Decimal):
            return _decimal_quantize_from_tol(v, float_tol)
    except Exception:
        pass

    # Bytes-like
    if isinstance(v, (bytes, bytearray, memoryview)):
        return (_BYTES_TOKEN, bytes(v).hex())

    # Datetime family (explicit types for static analyzers)
    try:
        import datetime as _dt
        if isinstance(v, (_dt.datetime, _dt.date, _dt.time)):
            return v.isoformat()
    except Exception:
        pass

    # NumPy scalar -> native Python type
    try:
        import numpy as np  # type: ignore
        if isinstance(v, np.generic):
            return v.item()
    except Exception:
        pass

    # Already JSON-serializable?
    try:
        json.dumps(v)
        return v
    except TypeError:
        return repr(v)


def normalize_row(row: Tuple, float_tol: float) -> Tuple:
    """Normalize every element in a row tuple."""
    return tuple(normalize_value(v, float_tol) for v in row)


def rows_to_bag(rows: List[Tuple], float_tol: float):
    """
    Convert a list of rows into a multiset (Counter) of normalized rows.
    Enables bag (multiset) comparison across engines.
    """
    import collections
    normd = [normalize_row(r, float_tol) for r in rows]
    return collections.Counter(normd)

# -------------------------------- Runners -----------------------------------

def run_sqlite(db_path: str, query: str) -> Tuple[List[str], List[Tuple]]:
    """
    Execute SQL on SQLite. Returns (column_names, rows).
    Non-SELECT statements are executed but ignored for result accumulation.
    """
    con = sqlite3.connect(db_path)
    try:
        con.isolation_level = None
        cur = con.cursor()
        rows_acc: List[Tuple] = []
        columns: Optional[List[str]] = None
        for stmt in split_statements(query):
            if is_select(stmt):
                cur.execute(stmt)
                if columns is None:
                    columns = [d[0] for d in (cur.description or [])]
                rows_acc = cur.fetchall()
            else:
                cur.execute(stmt)
        return (columns or []), rows_acc
    finally:
        try:
            con.close()
        except Exception:
            pass


def run_duckdb(db_path: str, query: str, threads: Optional[int] = None) -> Tuple[List[str], List[Tuple]]:
    """
    Execute SQL on DuckDB. Optionally set PRAGMA threads.
    Returns (column_names, rows).
    """
    con = duckdb.connect(database=db_path)
    try:
        if threads and threads > 0:
            try:
                con.execute(f"PRAGMA threads={int(threads)}")
            except Exception:
                pass
        rows_acc: List[Tuple] = []
        columns: Optional[List[str]] = None
        for stmt in split_statements(query):
            if is_select(stmt):
                cur = con.execute(stmt)
                if columns is None:
                    columns = [d[0] for d in (cur.description or [])]
                rows_acc = cur.fetchall()
            else:
                con.execute(stmt)
        return (columns or []), rows_acc
    finally:
        try:
            con.close()
        except Exception:
            pass


def run_chdb(db_path: str, query: str, threads: Optional[int] = None) -> Tuple[List[str], List[Tuple]]:
    """
    Execute SQL on ClickHouse via chdb. Optionally set max_threads.
    Returns (column_names, rows).
    """
    con = chdb.connect(db_path if db_path else ":memory:")
    try:
        cur = con.cursor()
        if threads and int(threads) > 0:
            cur.execute(f"SET max_threads = {int(threads)}")
        rows_acc: List[Tuple] = []
        columns: Optional[List[str]] = None
        for stmt in split_statements(query):
            cur.execute(stmt)
            if is_select(stmt):
                if columns is None and cur.description:
                    columns = [d[0] for d in cur.description]
                rows_acc = cur.fetchall()
            else:
                cur.execute(stmt)
        cur.close()
        return (columns or []), rows_acc
    finally:
        try:
            con.close()
        except Exception:
            pass


RUNNERS = {
    "duckdb": run_duckdb,
    "sqlite": run_sqlite,
    "chdb": run_chdb,
}

# -------------------------------- Compare -----------------------------------

def compare_results(
    a_cols: List[str],
    a_rows: List[Tuple],
    b_cols: List[str],
    b_rows: List[Tuple],
    mode: str,
    float_tol: float,
    ignore_colnames: bool,
    sample_limit: int = 10,
) -> Dict[str, Any]:
    """
    Compare results (columns and rows) from two engines.

    Returns a dict with:
    - ok: bool
    - column_issue: optional details if columns mismatch
    - row_issue: optional details for row diffs (ordered or bag)
    """
    result: Dict[str, Any] = {
        "ok": True,
        "column_issue": None,  # {"a_cols": [...], "b_cols": [...], "hints": [...]}
        "row_issue": None,     # {"mode": "ordered"/"bag", "detail": {...}}
    }

    # --- Column comparison ---
    def names_norm(cols: List[str]) -> List[str]:
        return [str(c or "").strip().lower() for c in cols]

    col_hints: List[str] = []
    if len(a_cols) != len(b_cols) or (not ignore_colnames and names_norm(a_cols) != names_norm(b_cols)):
        # Hints: strip table prefixes, simple alias heuristics
        def strip_prefix(c: str) -> str:
            return str(c).split(".", 1)[-1]

        if [strip_prefix(x) for x in a_cols] == [strip_prefix(x) for x in b_cols]:
            col_hints.append("Column name prefixes differ (e.g., 't.'). Consider aliasing without table prefixes.")

        alias_pairs = {("total_steps", "steps_sum"), ("steps_sum", "total_steps")}
        for ac, bc in zip(a_cols, b_cols):
            if (ac, bc) in alias_pairs:
                col_hints.append(f"Possible alias: '{ac}' ~ '{bc}'. Consider renaming to match.")
                break

        result["ok"] = False
        result["column_issue"] = {"a_cols": a_cols, "b_cols": b_cols, "hints": col_hints}

    # --- Row comparison ---
    if mode == "ordered":
        # Different row counts -> immediate fail
        if len(a_rows) != len(b_rows):
            result["ok"] = False
            result["row_issue"] = {
                "mode": "ordered",
                "detail": {"type": "row_count_mismatch", "a_count": len(a_rows), "b_count": len(b_rows)},
            }
        else:
            # Compare row-by-row after normalization
            def row_repr_seq(rows: List[Tuple]) -> List[str]:
                return [
                    json.dumps(normalize_row(r, float_tol), ensure_ascii=False, sort_keys=True)
                    for r in rows
                ]

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

# ---------------------------------- CLI -------------------------------------

def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    ap = argparse.ArgumentParser(description="Result equivalence checker across explicit ENGINE-DB-SQL cases")
    ap.add_argument(
        "--case", nargs=3, action="append",
        metavar=("ENGINE", "DB_PATH", "SQL_FILE"),
        help="One case: ENGINE DB_PATH SQL_FILE. Repeat for multiple cases.",
    )
    ap.add_argument("--threads", type=int, default=0, help="DuckDB/chDB threads")
    ap.add_argument("--mode", choices=["ordered", "bag"], default="bag", help="Equivalence model")
    ap.add_argument("--float-tol", type=float, default=DEFAULT_FLOAT_TOLERANCE, help="Floating point tolerance")
    ap.add_argument("--ignore-colnames", action="store_true", help="Ignore column name differences; only check count")
    ap.add_argument("--stop-on-first-fail", action="store_true", help="Stop at the first mismatch")
    ap.add_argument("--verbose", action="store_true", help="Verbose execution logs")
    ap.add_argument("--output", choices=["human", "json"], default="human", help="Output format")
    ap.add_argument("--show", type=int, default=DEFAULT_SAMPLE_LIMIT, help="Max number of sample row diffs to print")
    ap.add_argument("--json-file", type=str, default="", help="Also write full JSON summary to this path")

    args = ap.parse_args()

    if not args.case or len(args.case) < 2:
        print("Error: Provide at least two --case entries (ENGINE DB_PATH SQL_FILE).", file=sys.stderr)
        sys.exit(2)
    return args


def validate_and_run_cases(args: argparse.Namespace) -> List[Dict[str, Any]]:
    """
    Validate user inputs and execute each case. Returns a list of per-case dicts:
    {
      label, engine, db, sql_file, columns, row_count, rows, time_sec
    }
    """
    per_case: List[Dict[str, Any]] = []

    for (engine, dbp, sqlf) in args.case:
        eng = engine.strip().lower()

        # Engine selection
        if eng not in RUNNERS:
            print(f"Unknown engine '{engine}'. Must be one of: {list(RUNNERS)}", file=sys.stderr)
            sys.exit(2)

        # Files exist?
        if not os.path.exists(dbp):
            print(f"Database file not found: {dbp}", file=sys.stderr)
            sys.exit(2)
        if not os.path.exists(sqlf):
            print(f"SQL file not found: {sqlf}", file=sys.stderr)
            sys.exit(2)

        # Execute
        try:
            sql = load_query_from_file(sqlf)
            runner = RUNNERS[eng]
            t0 = time.perf_counter()

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
                print(f'[{eng}] rows={len(rows)} cols={len(cols)} time={t1 - t0:.3f}s on {sqlf}')
        except Exception as e:
            print(f"Error executing {eng} query: {e}", file=sys.stderr)
            sys.exit(2)

    return per_case


def compare_all_cases(per_case: List[Dict[str, Any]], args: argparse.Namespace) -> Tuple[bool, Dict[str, Any]]:
    """
    Perform pairwise comparisons across all cases. Returns:
    (any_fail: bool, comparisons: Dict[key, result])
    """
    any_fail = False
    comparisons: Dict[str, Any] = {}

    for i in range(len(per_case)):
        for j in range(i + 1, len(per_case)):
            a = per_case[i]
            b = per_case[j]

            comp = compare_results(
                a["columns"], a["rows"],
                b["columns"], b["rows"],
                mode=args.mode,
                float_tol=args.float_tol,
                ignore_colnames=args.ignore_colnames,
                sample_limit=args.show,
            )

            key = f'{a["label"]} ~ {b["label"]}'
            comparisons[key] = comp

            if not comp["ok"]:
                any_fail = True
                if args.stop_on_first_fail:
                    return any_fail, comparisons

    return any_fail, comparisons


def build_summary(per_case: List[Dict[str, Any]], comparisons: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    """Build a JSON-serializable summary object (without including raw rows)."""
    return {
        "mode": args.mode,
        "float_tol": args.float_tol,
        "ignore_colnames": args.ignore_colnames,
        "cases": [{k: v for k, v in pc.items() if k != "rows"} for pc in per_case],
        "comparisons": comparisons,
    }


def output_summary(summary: Dict[str, Any], comparisons: Dict[str, Any], args: argparse.Namespace) -> None:
    """Print either human-readable report or JSON summary to stdout."""
    if args.output == "json":
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

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


def main() -> None:
    """Main entrypoint: parse args, run cases, compare, and report."""
    args = _parse_args()

    # Run and collect per-case results
    per_case = validate_and_run_cases(args)

    # Pairwise compare
    any_fail, comparisons = compare_all_cases(per_case, args)

    # Build summary (without raw rows)
    summary = build_summary(per_case, comparisons, args)

    # Optional JSON dump to file
    if args.json_file:
        try:
            with open(args.json_file, "w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            if args.verbose:
                print(f"JSON summary written to: {args.json_file}")
        except Exception as e:
            print(f"Warning: Failed to write JSON file '{args.json_file}': {e}", file=sys.stderr)

    # Output to stdout
    output_summary(summary, comparisons, args)

    sys.exit(1 if any_fail else 0)


if __name__ == "__main__":
    main()
