#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Usage example:
  python benchmark.py --engine duckdb --db-path ./vs14_data.duckdb --query-file queries/Q1.sql
"""

import argparse, os, sys, threading, time
import psutil
import tracemalloc

from utils import load_query_from_file
from query_db import run_query


def monitor_process(pid, interval, out_dict, stop_event):
    p = psutil.Process(pid)
    peak_rss = 0
    cpu_series = []

    # Initialize CPU percentage baseline; first call usually returns 0, used as reference
    try:
        p.cpu_percent(interval=None)
    except Exception:
        pass

    while not stop_event.is_set():
        try:
            # Sample at fixed intervals
            time.sleep(interval)

            rss = p.memory_info().rss  # bytes
            peak_rss = max(peak_rss, rss)

            # CPU percentage since last call (may be >100%, indicating multi-core usage)
            cpu = p.cpu_percent(interval=None)
            cpu_series.append(cpu)

            # Write intermediate state back for main thread to read anytime
            out_dict["last_rss_bytes"] = rss
            out_dict["peak_rss_bytes"] = peak_rss
            out_dict["cpu_last_percent"] = cpu
            out_dict["cpu_avg_percent"] = (sum(cpu_series) / len(cpu_series))
            out_dict["samples"] = len(cpu_series)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            break

    # Write final results at the end
    out_dict["peak_rss_bytes"] = peak_rss
    out_dict["cpu_avg_percent"] = (sum(cpu_series) / len(cpu_series)) if cpu_series else 0.0
    out_dict["cpu_series"] = cpu_series


def mode_inproc(engine='duckdb', db_path=None, sample_interval=0.2, query=''):
    """
    Execute run_query() in the current process and record with psutil + tracemalloc:
      - Peak RSS (physical resident memory, bytes)
      - CPU percentage curve and average
      - Python heap peak (Python objects only)
    """
    stats = {}
    stop_event = threading.Event()
    mon = threading.Thread(target=monitor_process, args=(os.getpid(), sample_interval, stats, stop_event))

    tracemalloc.start()
    tracemalloc.reset_peak()

    t0 = time.perf_counter()
    mon.start()
    ret = run_query(engine, query, db_path)
    stop_event.set()
    mon.join()
    t1 = time.perf_counter()

    py_current, py_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    result = {
        "retval": ret,
        "wall_time_seconds": t1 - t0,
        "peak_rss_bytes": stats.get("peak_rss_bytes", 0),
        "cpu_avg_percent": stats.get("cpu_avg_percent", 0.0),
        "samples": stats.get("samples", len(stats.get("cpu_series", []))),
        "python_heap_peak_bytes": py_peak,  # Python heap only; DuckDB/SQLite C layer not included
    }
    return result


def main():
    ap = argparse.ArgumentParser(description="SQLite / DuckDB query CPU / memory measurement tool")
    ap.add_argument("--engine", choices=["duckdb", "sqlite"], default="duckdb", 
                    help="Choose database engine: duckdb or sqlite (default: duckdb)")
    ap.add_argument("--db-path", type=str, default="./data.duckdb",
                    help="Path to database file (.duckdb for DuckDB, .sqlite for SQLite)")
    ap.add_argument("--interval", type=float, default=0.2, help="Sampling interval seconds, default 0.2s")
    ap.add_argument("--query-file", type=str, default="", help="Path to SQL file to run (e.g., Q1)")
    args = ap.parse_args()

    db_path = args.db_path
    # Check if database file exists
    if not os.path.exists(db_path):
        print(f"Error: Database file not found: {db_path}")
        sys.exit(1)
   
    sample_query_file = "queries/sample.sql"
    if not os.path.exists(args.query_file):
        query = load_query_from_file(sample_query_file)
        print(f"Running query {sample_query_file} using {args.engine.upper()} engine with database: {db_path}")
    else:
        query = load_query_from_file(args.query_file)
        print(f"Running query {args.query_file} using {args.engine.upper()} engine with database: {db_path}")
    
    res = mode_inproc(engine=args.engine, db_path=db_path, sample_interval=args.interval, query=query)
    print("[{}] wall={:.3f}s  peak_rss={:.1f} MB  cpu_avg={:.1f}%  py_heap_peak={:.1f} MB  samples={}".format(
        args.engine.upper(),
        res["wall_time_seconds"],
        res["peak_rss_bytes"] / (1024**2),
        res["cpu_avg_percent"],
        res["python_heap_peak_bytes"] / (1024**2),
        res["samples"],
    ))

if __name__ == "__main__":
    main()
