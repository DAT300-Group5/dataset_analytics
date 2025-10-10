# benchmark/cli.py
import argparse
import os
import sys


def build_benchmark_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="SQLite / DuckDB / chDB benchmark with CPU/RSS/TTFR + P95/P99 (DB-internal memory metrics removed)")
    ap.add_argument("--engine", choices=["duckdb", "sqlite", "chdb"], default="duckdb",
                    help="Database engine: duckdb | sqlite | chdb (default: duckdb)")
    ap.add_argument("--db-path", type=str, required=True,
                    help="Path to database file/dir (.duckdb / .sqlite / chdb directory)")
    ap.add_argument("--interval", type=float, default=0.2,
                    help="Sampling interval in seconds (default: 0.2)")
    ap.add_argument("--query-file", type=str, required=False, default="",
                    help="Path to SQL file. If omitted, uses queries/sample.sql if present.")
    ap.add_argument("--repeat", type=int, default=10,
                    help="Number of measured runs (default: 10)")
    ap.add_argument("--out", type=str, default="",
                    help="If set, write all measured runs and summary as JSON to this path")
    return ap

def validate_benchmark_args(args: argparse.Namespace):
    # Mode validation
    # No additional validation needed for child vs inproc modes

    if not os.path.exists(args.db_path):
        print(f"Error: Database path not found: {args.db_path}", file=sys.stderr)
        sys.exit(1)

    query_file = args.query_file.strip()
    if query_file:
        if not os.path.exists(query_file):
            print(f"Error: Query file not found: {query_file}", file=sys.stderr)
            sys.exit(1)
    else:
        query_file = "queries/sample.sql"
        args.query_file = query_file
        if not os.path.exists(query_file):
            print("Error: no --query-file provided and default queries/sample.sql not found.", file=sys.stderr)
            sys.exit(1)
        print("[Info] No --query-file provided, falling back to queries/sample.sql")

def parse_benchmark_args(argv=None):
    args = build_benchmark_parser().parse_args(argv)
    validate_benchmark_args(args)
    return args