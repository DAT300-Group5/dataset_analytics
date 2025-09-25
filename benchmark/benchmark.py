#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Usage example:
  # TODO: modular support for different queries 
  python perf_probe.py
"""

import argparse, os, sys, threading, time
import psutil
import tracemalloc

# ---------- Import query modules ----------
from query_duckdb import run_query_duckdb
from query_sqlite import run_query_sqlite

def run_query(query_file, engine='duckdb', db_path=None):
    """
    Run query according to the specified engine
    """

    # TODO: right now example query, create support for actual queries - how would we store queries
    example_query = """
        SELECT 
            AVG(x) as avg_x
        FROM acc
    """

    if engine.lower() == 'duckdb':
        return run_query_duckdb(query_file, db_path)
    elif engine.lower() == 'sqlite':
        return run_query_sqlite(query_file, db_path)
    else:
        raise ValueError(f"Unsupported database engine: {engine}. Supported engines: duckdb, sqlite")
# -------------------------------------------------------

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

def mode_inproc(query_file, engine='duckdb', db_path=None, sample_interval=0.2):
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
    ret = run_query(engine=engine,db_path=db_path, query_file=query_file)
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
    ap.add_argument("--duckdb-path", type=str, default="./data_duckdb.db",
                    help="Path to DuckDB database file (default: ./data_duckdb.db)")
    ap.add_argument("--sqlite-path", type=str, default="./data_sqlite.db", 
                    help="Path to SQLite database file (default: ./data_sqlite.db)")
    ap.add_argument("--query-file", type=str, required=True, help="Path to SQL query file")
    ap.add_argument("--interval", type=float, default=0.2, help="Sampling interval seconds, default 0.2s")
    args = ap.parse_args()

    # Select the appropriate database path based on engine
    db_path = args.duckdb_path if args.engine == 'duckdb' else args.sqlite_path
    
    print(f"Running query using {args.engine.upper()} engine with database: {db_path}")
    res = mode_inproc(engine=args.engine, db_path=db_path, sample_interval=args.interval, query_file=args.query_file)
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