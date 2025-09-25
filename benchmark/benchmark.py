#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import argparse, os, sys, threading, time, json, math
import psutil
import tracemalloc
from multiprocessing import Process, Pipe
from utils import load_query_from_file
from query_db import run_query_with_ttfr

# -------------------------- percentile helpers --------------------------
def nearest_rank_percentile(values, p):
    """Nearest-rank percentile (common for latency)."""
    if not values:
        return None
    if p <= 0:
        return min(values)
    if p >= 100:
        return max(values)
    arr = sorted(values)
    k = math.ceil(p / 100.0 * len(arr))
    idx = max(1, k) - 1
    return arr[idx]

def summarize_percentiles(values):
    """Return mean/p50/p95/p99 for a list of numbers."""
    if not values:
        return {"mean": None, "p50": None, "p95": None, "p99": None}
    return {
        "mean": sum(values) / len(values),
        "p50": nearest_rank_percentile(values, 50),
        "p95": nearest_rank_percentile(values, 95),
        "p99": nearest_rank_percentile(values, 99),
    }

# -------------------------- OS-level monitor thread --------------------------
def monitor_process(pid, interval, out_dict, stop_event):
    """
    Periodically sample RSS and CPU% for the given PID.
    - Captures an immediate first sample (to reduce early-peak miss).
    - CPU% may exceed 100% on multi-core systems.
    - Attempts to retrieve true high-water RSS (VmHWM on Linux, peak working
      set on Windows); falls back to sampled peak if not available.
    """
    p = psutil.Process(pid)
    peak_rss = 0
    cpu_series = []

    # Establish CPU baseline; first call is a reference
    try:
        p.cpu_percent(interval=None)
    except Exception:
        pass

    def sample_once():
        nonlocal peak_rss
        try:
            rss = p.memory_info().rss
            peak_rss = max(peak_rss, rss)
            cpu = p.cpu_percent(interval=None)  # may be >100% on multi-core
            cpu_series.append(cpu)

            out_dict["last_rss_bytes"] = rss
            out_dict["peak_rss_bytes"] = peak_rss
            out_dict["cpu_last_percent"] = cpu
            out_dict["cpu_avg_percent"] = (sum(cpu_series) / len(cpu_series))
            out_dict["samples"] = len(cpu_series)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False
        return True

    # First immediate sample
    sample_once()

    while not stop_event.is_set():
        time.sleep(interval)
        if not sample_once():
            break

    # Finalize CPU average and attempt true high-water RSS
    out_dict["cpu_series"] = cpu_series
    out_dict["cpu_avg_percent"] = (sum(cpu_series) / len(cpu_series)) if cpu_series else 0.0

    try:
        if sys.platform.startswith("linux"):
            with open(f"/proc/{pid}/status", "r") as f:
                for line in f:
                    if line.startswith("VmHWM:"):
                        parts = line.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            vmhwm_kb = int(parts[1])
                            out_dict["peak_rss_bytes_true"] = vmhwm_kb * 1024
                        break
        elif sys.platform.startswith("win"):
            full = p.memory_full_info()
            if hasattr(full, "peak_wset"):
                out_dict["peak_rss_bytes_true"] = full.peak_wset
    except Exception:
        pass

    out_dict.setdefault("peak_rss_bytes_true", out_dict.get("peak_rss_bytes", 0))

# -------------------------- DB-internal memory helpers --------------------------
def fetch_sqlite_memory_metrics(db_path, sqlite_pragmas=None):
    """
    Returns a dict:
      {
        "db_memory_used_bytes": <int or None>,
        "db_memory_highwater_bytes": <int or None>
      }
    Uses: PRAGMA memory_used; PRAGMA memory_highwater;
    These reflect SQLite library allocations (not Python, not OS page cache).
    """
    try:
        import sqlite3
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        # Apply PRAGMAs if provided (not strictly needed for memory_* pragmas)
        if sqlite_pragmas:
            for k, v in sqlite_pragmas.items():
                try:
                    cur.execute(f"PRAGMA {k}={v}")
                except Exception:
                    pass
        cur.execute("PRAGMA memory_used;")
        used = cur.fetchone()
        used_val = int(used[0]) if used and used[0] is not None else None

        # memory_highwater can optionally accept a reset flag, we do not reset here
        cur.execute("PRAGMA memory_highwater;")
        hwm = cur.fetchone()
        hwm_val = int(hwm[0]) if hwm and hwm[0] is not None else None

        cur.close()
        conn.close()
        return {
            "db_memory_used_bytes": used_val,
            "db_memory_highwater_bytes": hwm_val
        }
    except Exception:
        return {
            "db_memory_used_bytes": None,
            "db_memory_highwater_bytes": None
        }

def _sum_duckdb_memory_usage_rows(rows):
    """
    PRAGMA memory_usage returns a table, but schema may vary across versions.
    We try to best-effort aggregate a single numeric 'total bytes' by summing
    all integer-like cells from all rows.
    If there is a single numeric cell overall, we use that; otherwise we sum.
    """
    if not rows:
        return None
    numeric_vals = []
    for r in rows:
        # r may be tuple-like
        for cell in (r if isinstance(r, (tuple, list)) else [r]):
            try:
                # accept ints; also accept numeric strings
                if isinstance(cell, (int, float)):
                    numeric_vals.append(int(cell))
                else:
                    # try parse strings like "12345"
                    if isinstance(cell, str) and cell.strip().isdigit():
                        numeric_vals.append(int(cell.strip()))
            except Exception:
                pass
    if not numeric_vals:
        return None
    # Heuristic: if only one numeric value found, use it; else sum them
    return numeric_vals[0] if len(numeric_vals) == 1 else sum(numeric_vals)

def fetch_duckdb_memory_metrics(db_path, duckdb_threads=None):
    """
    Returns a dict:
      {
        "db_memory_usage_bytes": <int or None>
      }
    We try to query PRAGMA memory_usage and reduce it to one number.
    This is best-effort and may depend on DuckDB version.
    """
    try:
        import duckdb
        con = duckdb.connect(db_path)
        if duckdb_threads and duckdb_threads > 0:
            con.execute(f"PRAGMA threads={duckdb_threads}")
        # Some versions support PRAGMA memory_usage returning a table
        rows = con.execute("PRAGMA memory_usage").fetchall()
        total = _sum_duckdb_memory_usage_rows(rows)
        con.close()
        return {"db_memory_usage_bytes": total}
    except Exception:
        return {"db_memory_usage_bytes": None}

# -------------------------- one-shot (inproc) single run --------------------------
def run_once_inproc(engine='duckdb', db_path=None, sample_interval=0.2, query='',
                    duckdb_threads=None, sqlite_pragmas=None):
    """
    Single measured run in the current process:
      - Monitor this process (PID) with psutil (CPU/RSS).
      - Measure wall time and TTFR.
      - Collect Python heap peak via tracemalloc (Python objects only).
      - Fetch DB-internal memory metrics AFTER the query returns.
    """
    stats = {}
    stop_event = threading.Event()
    mon = threading.Thread(target=monitor_process,
                           args=(os.getpid(), sample_interval, stats, stop_event),
                           daemon=True)

    tracemalloc.start()
    tracemalloc.reset_peak()

    t0 = time.perf_counter()
    mon.start()
    try:
        exec_info = run_query_with_ttfr(engine, db_path, query,
                                        duckdb_threads=duckdb_threads,
                                        sqlite_pragmas=sqlite_pragmas)
        # Fetch DB-internal memory metrics at end of run
        if engine == 'sqlite':
            dbmem = fetch_sqlite_memory_metrics(db_path, sqlite_pragmas)
        else:
            dbmem = fetch_duckdb_memory_metrics(db_path, duckdb_threads)
    finally:
        stop_event.set()
        mon.join(timeout=5)
        py_current, py_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    t1 = time.perf_counter()

    out = {
        "retval": exec_info["retval"],
        "wall_time_seconds": t1 - t0,
        "ttfr_seconds": exec_info["first_select_ttfr_seconds"],
        "rows_returned": exec_info["rows_returned"],
        "statements_executed": exec_info["statements_executed"],
        "select_statements": exec_info["select_statements"],
        "peak_rss_bytes_sampled": stats.get("peak_rss_bytes", 0),
        "peak_rss_bytes_true": stats.get("peak_rss_bytes_true", stats.get("peak_rss_bytes", 0)),
        "cpu_avg_percent": stats.get("cpu_avg_percent", 0.0),
        "samples": stats.get("samples", 0),
        "python_heap_peak_bytes": py_peak,  # Python only; DB C layer excluded
        "mode": "inproc",
    }
    out.update(dbmem or {})
    return out

# -------------------------- per-run child (existing) --------------------------
def _child_worker_oneshot(conn, engine, db_path, query, duckdb_threads, sqlite_pragmas):
    """
    Child process entry for one-shot child mode:
      - Run the query and compute TTFR.
      - Trace Python heap peak (child-only).
      - Query DB-internal memory metrics after run.
      - Return exec_info + python_heap_peak + child_wall + dbmem via Pipe.
    """
    try:
        tracemalloc.start()
        tracemalloc.reset_peak()
        t0 = time.perf_counter()
        exec_info = run_query_with_ttfr(engine, db_path, query,
                                        duckdb_threads=duckdb_threads,
                                        sqlite_pragmas=sqlite_pragmas)
        # DB memory metrics inside child
        if engine == 'sqlite':
            dbmem = fetch_sqlite_memory_metrics(db_path, sqlite_pragmas)
        else:
            dbmem = fetch_duckdb_memory_metrics(db_path, duckdb_threads)

        t1 = time.perf_counter()
        py_current, py_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        payload = {
            "ok": True,
            "exec_info": exec_info,
            "child_wall_time_seconds": t1 - t0,
            "child_python_heap_peak_bytes": py_peak,
            "dbmem": dbmem
        }
        conn.send(payload)
    except Exception as e:
        conn.send({"ok": False, "error": str(e)})
    finally:
        conn.close()

def run_once_child(engine='duckdb', db_path=None, sample_interval=0.2, query='',
                   duckdb_threads=None, sqlite_pragmas=None):
    """
    One-shot child mode:
      - Fork a child process for this run only.
      - Parent monitors child PID; child measures TTFR & child wall & db memory.
    """
    parent_conn, child_conn = Pipe(duplex=False)
    p = Process(target=_child_worker_oneshot,
                args=(child_conn, engine, db_path, query, duckdb_threads, sqlite_pragmas),
                daemon=False)
    p.start()

    stats = {}
    stop_event = threading.Event()
    mon = threading.Thread(target=monitor_process,
                           args=(p.pid, sample_interval, stats, stop_event),
                           daemon=True)
    t0 = time.perf_counter()
    mon.start()

    payload = None
    try:
        payload = parent_conn.recv()
    finally:
        p.join()
        stop_event.set()
        mon.join(timeout=5)
        t1 = time.perf_counter()
        parent_conn.close()

    if not payload or not payload.get("ok", False):
        err = (payload or {}).get("error", "unknown child error")
        raise RuntimeError(f"Child process failed: {err}")

    exec_info = payload["exec_info"]
    out = {
        "retval": exec_info["retval"],
        "wall_time_seconds": t1 - t0,  # parent-observed wall
        "ttfr_seconds": exec_info["first_select_ttfr_seconds"],
        "rows_returned": exec_info["rows_returned"],
        "statements_executed": exec_info["statements_executed"],
        "select_statements": exec_info["select_statements"],
        "peak_rss_bytes_sampled": stats.get("peak_rss_bytes", 0),
        "peak_rss_bytes_true": stats.get("peak_rss_bytes_true", stats.get("peak_rss_bytes", 0)),
        "cpu_avg_percent": stats.get("cpu_avg_percent", 0.0),
        "samples": stats.get("samples", 0),
        "python_heap_peak_bytes": payload["child_python_heap_peak_bytes"],
        "child_wall_time_seconds": payload["child_wall_time_seconds"],
        "mode": "child",
    }
    out.update(payload.get("dbmem") or {})
    return out

# -------------------------- persistent child (new) --------------------------
def _child_worker_persistent(conn, engine, db_path, duckdb_threads, sqlite_pragmas):
    """
    Persistent child process:
    - Creates ONE DB connection in the child.
    - Waits for commands from parent:
        {"cmd": "RUN", "query": <sql>}
        {"cmd": "EXIT"}
    - For each RUN:
        * tracemalloc: reset peak
        * measure child wall around run_query_with_ttfr()
        * after run, fetch DB memory metrics (PRAGMA memory_*) inside child
        * send payload back
    - On EXIT: break loop and return.
    """
    try:
        import sqlite3, duckdb  # may or may not be used depending on engine
    except Exception:
        pass

    # Prepare persistent context if needed; run_query_with_ttfr will open/use
    # its own connection internally. If you want to FOR SURE reuse the same
    # connection, you can refactor run_query_with_ttfr to accept a connection.
    # For now, we keep it simple and rely on engine-level caches that persist
    # within the child process; additionally we query PRAGMAs in this process.
    while True:
        msg = conn.recv()
        if not isinstance(msg, dict) or "cmd" not in msg:
            conn.send({"ok": False, "error": "bad command"})
            continue

        if msg["cmd"] == "EXIT":
            conn.send({"ok": True, "bye": True})
            break

        if msg["cmd"] == "RUN":
            query = msg.get("query", "")
            try:
                tracemalloc.start()
                tracemalloc.reset_peak()
                t0 = time.perf_counter()
                exec_info = run_query_with_ttfr(engine, db_path, query,
                                                duckdb_threads=duckdb_threads,
                                                sqlite_pragmas=sqlite_pragmas)
                # DB memory after run
                if engine == 'sqlite':
                    dbmem = fetch_sqlite_memory_metrics(db_path, sqlite_pragmas)
                else:
                    dbmem = fetch_duckdb_memory_metrics(db_path, duckdb_threads)
                t1 = time.perf_counter()
                py_current, py_peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()

                conn.send({
                    "ok": True,
                    "exec_info": exec_info,
                    "child_wall_time_seconds": t1 - t0,
                    "child_python_heap_peak_bytes": py_peak,
                    "dbmem": dbmem
                })
            except Exception as e:
                conn.send({"ok": False, "error": str(e)})
        else:
            conn.send({"ok": False, "error": f"unknown cmd {msg['cmd']}"})

    conn.close()

def run_persistent_child_session(engine, db_path, sample_interval, queries,
                                 duckdb_threads=None, sqlite_pragmas=None):
    """
    Runs multiple queries (warmups + repeats) against ONE persistent child process.

    Args:
      - queries: list of SQL texts (strings) to run in order; typically
                 [warmup]*W + [measured]*R, OR just the same query repeated.

    Returns:
      - results: list of per-run dicts (same schema as other run modes)
    """
    parent_conn, child_conn = Pipe(duplex=True)
    p = Process(target=_child_worker_persistent,
                args=(child_conn, engine, db_path, duckdb_threads, sqlite_pragmas),
                daemon=False)
    p.start()

    results = []
    try:
        for q in queries:
            # Start monitoring this child PID for this run
            stats = {}
            stop_event = threading.Event()
            mon = threading.Thread(target=monitor_process,
                                   args=(p.pid, sample_interval, stats, stop_event),
                                   daemon=True)

            t0 = time.perf_counter()
            mon.start()

            # Ask child to run
            parent_conn.send({"cmd": "RUN", "query": q})
            payload = parent_conn.recv()

            # Finalize monitoring
            stop_event.set()
            mon.join(timeout=5)
            t1 = time.perf_counter()

            if not payload or not payload.get("ok", False):
                err = (payload or {}).get("error", "unknown child error")
                raise RuntimeError(f"Persistent child run failed: {err}")

            exec_info = payload["exec_info"]
            out = {
                "retval": exec_info["retval"],
                "wall_time_seconds": t1 - t0,  # parent-observed per-run wall
                "ttfr_seconds": exec_info["first_select_ttfr_seconds"],
                "rows_returned": exec_info["rows_returned"],
                "statements_executed": exec_info["statements_executed"],
                "select_statements": exec_info["select_statements"],
                "peak_rss_bytes_sampled": stats.get("peak_rss_bytes", 0),
                "peak_rss_bytes_true": stats.get("peak_rss_bytes_true", stats.get("peak_rss_bytes", 0)),
                "cpu_avg_percent": stats.get("cpu_avg_percent", 0.0),
                "samples": stats.get("samples", 0),
                "python_heap_peak_bytes": payload["child_python_heap_peak_bytes"],
                "child_wall_time_seconds": payload["child_wall_time_seconds"],
                "mode": "child-persistent",
            }
            out.update(payload.get("dbmem") or {})
            results.append(out)

        # Ask child to exit
        parent_conn.send({"cmd": "EXIT"})
        _ = parent_conn.recv()  # ignore content
    finally:
        p.join(timeout=5)
        if p.is_alive():
            p.kill()
        parent_conn.close()

    return results

# -------------------------- summarize helpers --------------------------
def _collect(runs, key, allow_none=False):
    vals = [r.get(key) for r in runs if allow_none or r.get(key) is not None]
    if not allow_none:
        vals = [v for v in vals if v is not None]
    return vals

def _mean_or_none(lst):
    return (sum(lst) / len(lst)) if lst else None

# -------------------------------------- main --------------------------------------
def main():
    ap = argparse.ArgumentParser(description="SQLite / DuckDB benchmark with CPU/RSS/TTFR + P95/P99 + DB memory + persistent child")
    ap.add_argument("--engine", choices=["duckdb", "sqlite"], default="duckdb",
                    help="Database engine: duckdb or sqlite (default: duckdb)")
    ap.add_argument("--db-path", type=str, required=True,
                    help="Path to database file (.duckdb or .sqlite)")
    ap.add_argument("--interval", type=float, default=0.2,
                    help="Sampling interval in seconds (default: 0.2)")
    ap.add_argument("--query-file", type=str, required=False, default="",
                    help="Path to SQL file. If omitted, uses queries/sample.sql if present.")
    ap.add_argument("--repeat", type=int, default=10,
                    help="Number of measured runs (default: 10)")
    ap.add_argument("--warmups", type=int, default=0,
                    help="Number of warm-up runs not recorded (default: 0)")

    # Run mode switches (mutually exclusive): inproc | child | child-persistent
    ap.add_argument("--child", action="store_true",
                    help="Run each measured run in a separate child process")
    ap.add_argument("--child-persistent", action="store_true",
                    help="Run ALL warmups + repeats against ONE persistent child/connection")

    # DuckDB/SQLite knobs
    ap.add_argument("--threads", type=int, default=0,
                    help="DuckDB PRAGMA threads (0=engine default)")
    ap.add_argument("--sqlite-journal", type=str, default="",
                    help="SQLite PRAGMA journal_mode (e.g., WAL, OFF, DELETE)")
    ap.add_argument("--sqlite-sync", type=str, default="",
                    help="SQLite PRAGMA synchronous (OFF|NORMAL|FULL|EXTRA)")
    ap.add_argument("--sqlite-cache-size", type=int, default=0,
                    help="SQLite PRAGMA cache_size (pages; negative means KiB)")

    # Output
    ap.add_argument("--out", type=str, default="",
                    help="If set, write all measured runs and summary as JSON to this path")

    args = ap.parse_args()

    # Mode validation
    if args.child and args.child_persistent:
        print("Error: --child and --child-persistent are mutually exclusive.", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(args.db_path):
        print(f"Error: Database file not found: {args.db_path}", file=sys.stderr)
        sys.exit(1)

    query_file = args.query_file.strip()
    if query_file:
        if not os.path.exists(query_file):
            print(f"Error: Query file not found: {query_file}", file=sys.stderr)
            sys.exit(1)
    else:
        query_file = "queries/sample.sql"
        if not os.path.exists(query_file):
            print("Error: no --query-file provided and default queries/sample.sql not found.", file=sys.stderr)
            sys.exit(1)
        print("[Info] No --query-file provided, falling back to queries/sample.sql")

    query = load_query_from_file(query_file)

    # Build SQLite PRAGMAs
    sqlite_pragmas = {}
    if args.sqlite_journal:
        sqlite_pragmas["journal_mode"] = args.sqlite_journal
    if args.sqlite_sync:
        sqlite_pragmas["synchronous"] = args.sqlite_sync
    if args.sqlite_cache_size:
        sqlite_pragmas["cache_size"] = args.sqlite_cache_size

    # ---------------- Warmups ----------------
    if args.child_persistent:
        # In persistent mode we will run warmups + repeats in one session below.
        warmup_runs = []
    else:
        # Run warmups in the chosen non-persistent mode and discard results
        for i in range(args.warmups):
            _ = (run_once_child if args.child else run_once_inproc)(
                engine=args.engine,
                db_path=args.db_path,
                sample_interval=args.interval,
                query=query,
                duckdb_threads=(args.threads if args.threads > 0 else None),
                sqlite_pragmas=sqlite_pragmas,
            )
            print(f"[Warmup {i+1}/{args.warmups}] done.")

    # ---------------- Measured runs ----------------
    runs = []

    if args.child_persistent:
        # Build the sequence of queries to run inside ONE persistent child:
        # we still execute warmups (not recorded) followed by repeats (recorded)
        total_count = args.warmups + args.repeat
        queries = [query] * total_count
        results = run_persistent_child_session(
            engine=args.engine,
            db_path=args.db_path,
            sample_interval=args.interval,
            queries=queries,
            duckdb_threads=(args.threads if args.threads > 0 else None),
            sqlite_pragmas=sqlite_pragmas,
        )
        # Discard warmups, keep repeats
        warmups_dropped = results[:args.warmups]
        runs = results[args.warmups:]
        for i, res in enumerate(runs, 1):
            print(("[{eng}] {mode} run {k}/{n} wall={wall:.3f}s  child_wall={cwall:.3f}s  "
                   "ttfr={ttfr}  peak_rss_sampled={rss_s:.1f} MB  peak_rss_true={rss_t:.1f} MB  "
                   "cpu_avg={cpu:.1f}%  py_heap_peak={py:.1f} MB  rows={rows}  sel={sels}/{stmts}  samples={smp}  "
                   "db_used={dbu}  db_hwm={dbh}  db_usage={dbug}")
                  .format(
                      eng=args.engine.upper(),
                      mode="CHILD-PERSISTENT",
                      k=i, n=len(runs),
                      wall=res["wall_time_seconds"],
                      cwall=res.get("child_wall_time_seconds", 0.0),
                      ttfr=("%.3f s" % res["ttfr_seconds"]) if res["ttfr_seconds"] is not None else "None",
                      rss_s=res["peak_rss_bytes_sampled"] / (1024**2),
                      rss_t=res["peak_rss_bytes_true"] / (1024**2),
                      cpu=res["cpu_avg_percent"],
                      py=res["python_heap_peak_bytes"] / (1024**2),
                      rows=res["rows_returned"],
                      sels=res["select_statements"], stmts=res["statements_executed"],
                      smp=res["samples"],
                      dbu=res.get("db_memory_used_bytes"),
                      dbh=res.get("db_memory_highwater_bytes"),
                      dbug=res.get("db_memory_usage_bytes"),
                  ))
    else:
        for i in range(args.repeat):
            res = (run_once_child if args.child else run_once_inproc)(
                engine=args.engine,
                db_path=args.db_path,
                sample_interval=args.interval,
                query=query,
                duckdb_threads=(args.threads if args.threads > 0 else None),
                sqlite_pragmas=sqlite_pragmas,
            )
            runs.append(res)
            print(("[{eng}] {mode} run {k}/{n} wall={wall:.3f}s  {child_wall}  "
                   "ttfr={ttfr}  peak_rss_sampled={rss_s:.1f} MB  peak_rss_true={rss_t:.1f} MB  "
                   "cpu_avg={cpu:.1f}%  py_heap_peak={py:.1f} MB  rows={rows}  sel={sels}/{stmts}  samples={smp}  "
                   "db_used={dbu}  db_hwm={dbh}  db_usage={dbug}")
                  .format(
                      eng=args.engine.upper(),
                      mode=("CHILD" if args.child else "INPROC"),
                      k=i+1, n=args.repeat,
                      wall=res["wall_time_seconds"],
                      child_wall=("child_wall=%.3fs" % res["child_wall_time_seconds"]) if "child_wall_time_seconds" in res else "",
                      ttfr=("%.3f s" % res["ttfr_seconds"]) if res["ttfr_seconds"] is not None else "None",
                      rss_s=res["peak_rss_bytes_sampled"] / (1024**2),
                      rss_t=res["peak_rss_bytes_true"] / (1024**2),
                      cpu=res["cpu_avg_percent"],
                      py=res["python_heap_peak_bytes"] / (1024**2),
                      rows=res["rows_returned"],
                      sels=res["select_statements"], stmts=res["statements_executed"],
                      smp=res["samples"],
                      dbu=res.get("db_memory_used_bytes"),
                      dbh=res.get("db_memory_highwater_bytes"),
                      dbug=res.get("db_memory_usage_bytes"),
                  ))

    # ---------------- Aggregation ----------------
    wall_list = _collect(runs, "wall_time_seconds")
    ttfr_list = _collect(runs, "ttfr_seconds")
    rss_true_list = _collect(runs, "peak_rss_bytes_true")
    cpu_avg_list = _collect(runs, "cpu_avg_percent")
    rows_list = _collect(runs, "rows_returned")

    # DB memory lists (some may be None depending on engine/version)
    db_used_list = _collect(runs, "db_memory_used_bytes")
    db_hwm_list = _collect(runs, "db_memory_highwater_bytes")
    db_usage_list = _collect(runs, "db_memory_usage_bytes")

    wall_summary = summarize_percentiles(wall_list)
    ttfr_summary = summarize_percentiles(ttfr_list) if ttfr_list else {"mean": None, "p50": None, "p95": None, "p99": None}

    summary = {
        "engine": args.engine,
        "mode": "child-persistent" if args.child_persistent else ("child" if args.child else "inproc"),
        "db_path": args.db_path,
        "query_file": query_file,
        "repeat": args.repeat,
        "warmups": args.warmups,
        "threads": args.threads,
        "sqlite_pragmas": sqlite_pragmas,
        # Means
        "mean_wall_time_seconds": wall_summary["mean"],
        "mean_ttfr_seconds": ttfr_summary["mean"],
        "mean_peak_rss_bytes_true": _mean_or_none(rss_true_list),
        "mean_cpu_avg_percent": _mean_or_none(cpu_avg_list),
        "mean_rows_returned": _mean_or_none(rows_list),
        # Percentiles
        "p50_wall_time_seconds": wall_summary["p50"],
        "p95_wall_time_seconds": wall_summary["p95"],
        "p99_wall_time_seconds": wall_summary["p99"],
        "p50_ttfr_seconds": ttfr_summary["p50"],
        "p95_ttfr_seconds": ttfr_summary["p95"],
        "p99_ttfr_seconds": ttfr_summary["p99"],
        # DB memory means (as available)
        "mean_db_memory_used_bytes": _mean_or_none(db_used_list),
        "mean_db_memory_highwater_bytes": _mean_or_none(db_hwm_list),
        "mean_db_memory_usage_bytes": _mean_or_none(db_usage_list),
    }

    print("\n=== Summary ===")
    def fmt(v, unit="s"):
        return "None" if v is None else f"{v:.3f}{unit}"
    print("wall: mean={}  p50={}  p95={}  p99={}".format(
        fmt(summary["mean_wall_time_seconds"]),
        fmt(summary["p50_wall_time_seconds"]),
        fmt(summary["p95_wall_time_seconds"]),
        fmt(summary["p99_wall_time_seconds"]),
    ))
    print("ttfr: mean={}  p50={}  p95={}  p99={}".format(
        fmt(summary["mean_ttfr_seconds"]),
        fmt(summary["p50_ttfr_seconds"]),
        fmt(summary["p95_ttfr_seconds"]),
        fmt(summary["p99_ttfr_seconds"]),
    ))
    if rss_true_list:
        print("peak_rss_true: mean={:.1f} MB".format(summary["mean_peak_rss_bytes_true"] / (1024**2)))
    if cpu_avg_list:
        print("cpu_avg_percent: mean={:.1f}%".format(summary["mean_cpu_avg_percent"]))
    if rows_list:
        print("rows_returned: mean={:.1f}".format(summary["mean_rows_returned"]))
    if db_used_list or db_hwm_list or db_usage_list:
        print("db_memory_used_bytes: mean={}".format(summary["mean_db_memory_used_bytes"]))
        print("db_memory_highwater_bytes: mean={}".format(summary["mean_db_memory_highwater_bytes"]))
        print("db_memory_usage_bytes: mean={}".format(summary["mean_db_memory_usage_bytes"]))

    if args.out:
        out_obj = {"runs": runs, "summary": summary}
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out_obj, f, ensure_ascii=False, indent=2)
        print(f"[Info] Results written to {args.out}")

if __name__ == "__main__":
    main()