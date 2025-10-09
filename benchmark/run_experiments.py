#!/usr/bin/env python3
"""
Benchmark experiment runner for database performance testing.

This module orchestrates benchmark experiments across multiple database engines,
datasets, and query configurations.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from config import BenchmarkConfig, Dataset, QueryGroup
from models import BenchmarkResult

# Constants
HERE = Path(__file__).parent.resolve()
PROJECT = HERE.parent.resolve()

DEFAULT_PILOT_INTERVAL = 0.2
DEFAULT_FALLBACK_INTERVAL = 0.1
MIN_UNIFIED_INTERVAL = 0.05  # Minimum interval for unified intervals
BYTES_PER_MB = 1024 ** 2
PERCENT_MULTIPLIER = 100.0

SUPPORTED_MODES = {"child", "inproc"}


class BenchmarkRunner:
    """
    Manages execution of benchmark runs and pilot tests.
    
    Handles running individual benchmarks and calculating optimal intervals.
    """
    
    def __init__(self, config: BenchmarkConfig, results_dir: Path):
        """
        Initialize benchmark runner.
        
        Args:
            config: Benchmark configuration
            results_dir: Directory to store results
        """
        self.config = config
        self.results_dir = results_dir
        self.results_dir.mkdir(parents=True, exist_ok=True)
    
    def run_pilot(
        self,
        engine: str,
        dataset: Dataset,
        query_group: QueryGroup
    ) -> float:
        """
        Run pilot benchmark to determine optimal interval.
        
        Args:
            engine: Database engine name
            dataset: Dataset configuration
            query_group: Query group configuration
        
        Returns:
            Calculated interval for full run
        """
        db_path = self.config.get_database_path(dataset, engine)
        sql_path = self.config.get_sql_path(query_group, engine)
        pilot_out = self.results_dir / f"pilot__{dataset.name}__{query_group.id}__{engine}.json"
        
        try:
            run_bench(
                engine=engine,
                db_path=db_path,
                sql_path=sql_path,
                mode=self.config.mode,
                threads=self.config.get_threads(engine),
                repeat=self.config.repeat_pilot,
                warmups=self.config.warmups_pilot,
                interval=DEFAULT_PILOT_INTERVAL,
                out_json=pilot_out
            )
            
            # Load result using the new model class
            benchmark_result = BenchmarkResult.load_from_file(pilot_out)
            
            mean_wall = benchmark_result.summary.mean_wall_time_seconds
            return pick_interval(
                mean_wall,
                self.config.target_samples,
                self.config.min_interval,
                self.config.max_interval
            )
        
        except Exception as e:
            print(f"[WARN] pilot failed for {engine} {dataset.name} {query_group.id}: {e}")
            return DEFAULT_PILOT_INTERVAL
    
    def run_full_benchmark(
        self,
        engine: str,
        dataset: Dataset,
        query_group: QueryGroup,
        interval: float
    ) -> Path:
        """
        Run full benchmark with specified interval.
        
        Args:
            engine: Database engine name
            dataset: Dataset configuration
            query_group: Query group configuration
            interval: Time interval between runs
        
        Returns:
            Path to output JSON file
        """
        db_path = self.config.get_database_path(dataset, engine)
        sql_path = self.config.get_sql_path(query_group, engine)
        out_path = self.results_dir / f"{dataset.name}__{query_group.id}__{engine}.json"
        
        run_bench(
            engine=engine,
            db_path=db_path,
            sql_path=sql_path,
            mode=self.config.mode,
            threads=self.config.get_threads(engine),
            repeat=self.config.repeat_full,
            warmups=self.config.warmups_full,
            interval=interval,
            out_json=out_path
        )
        
        return out_path
    
    def choose_unified_interval(self, intervals: List[float]) -> float:
        """
        Choose a unified interval from multiple pilot test results.
        
        Uses the maximum interval to ensure all engines have sufficient time
        between runs for fair comparison. Enforces a minimum interval of 0.05
        to ensure stable measurements even for very fast queries.
        
        Args:
            intervals: List of intervals calculated from pilot runs
        
        Returns:
            Unified interval for all engines in the query group
        """
        if not intervals:
            return max(DEFAULT_PILOT_INTERVAL, MIN_UNIFIED_INTERVAL)
        
        # Filter out any None or invalid intervals
        valid_intervals = [i for i in intervals if i is not None and i > 0]
        
        if not valid_intervals:
            return max(DEFAULT_PILOT_INTERVAL, MIN_UNIFIED_INTERVAL)
        
        # Use maximum interval to ensure all engines have enough time
        max_interval = max(valid_intervals)
        
        # Enforce minimum interval for stable measurements
        unified_interval = max(max_interval, MIN_UNIFIED_INTERVAL)
        
        print(f"[INFO] Pilot intervals: {valid_intervals} -> max: {max_interval:.6f} -> unified: {unified_interval:.6f}")
        return unified_interval


def pick_interval(
    mean_wall: Optional[float],
    target_samples: int,
    min_interval: float,
    max_interval: float
) -> float:
    """
    Calculate optimal interval between benchmark runs based on execution time.
    
    Args:
        mean_wall: Mean wall clock time from pilot run (seconds)
        target_samples: Target number of samples
        min_interval: Minimum allowed interval
        max_interval: Maximum allowed interval
    
    Returns:
        Calculated interval between runs
    """
    if mean_wall is None or mean_wall <= 0:
        return max(min_interval, DEFAULT_FALLBACK_INTERVAL)
    
    raw = float(mean_wall) / float(target_samples)
    return max(min_interval, min(max_interval, raw))


def run_bench(
    engine: str,
    db_path: str,
    sql_path: str,
    mode: str,
    threads: int,
    repeat: int,
    warmups: int,
    interval: float,
    out_json: Path
) -> None:
    """
    Execute a benchmark run for a specific engine configuration.
    
    Args:
        engine: Database engine name (duckdb, sqlite, chdb)
        db_path: Path to database file or directory
        sql_path: Path to SQL query file
        mode: Execution mode (child, inproc)
        threads: Number of threads to use
        repeat: Number of repetitions
        warmups: Number of warmup runs
        interval: Time interval between runs
        out_json: Output path for results JSON file
    
    Raises:
        ValueError: If mode is not supported
        subprocess.CalledProcessError: If benchmark execution fails
    """
    args = [
        sys.executable, str("benchmark.py"),
        "--engine", engine,
        "--db-path", db_path,
        "--query-file", sql_path,
        "--repeat", str(repeat),
        "--warmups", str(warmups),
        "--interval", str(interval),
        "--out", str(out_json),
    ]
    if mode not in SUPPORTED_MODES:
        raise ValueError(f"Unknown mode '{mode}'. Supported modes: {SUPPORTED_MODES}")
    
    if mode == "child":
        args.append("--child")
    elif mode == "inproc":
        pass
    
    if threads and int(threads) > 0:
        args += ["--threads", str(threads)]
    
    print("[RUN]", " ".join(args))
    subprocess.run(args, check=True)


class ResultsProcessor:
    """
    Processes and exports benchmark results.
    
    Handles collection of benchmark results and export to CSV manifest.
    """
    
    def __init__(self, results_dir: Path):
        """
        Initialize results processor.
        
        Args:
            results_dir: Directory containing result files
        """
        self.results_dir = results_dir
        self.rows = []
    
    def add_result(
        self,
        dataset: Dataset,
        query_group: QueryGroup,
        engine: str,
        interval: float,
        result_path: Path
    ) -> None:
        """
        Add a benchmark result to the collection.
        
        Args:
            dataset: Dataset configuration
            query_group: Query group configuration
            engine: Database engine name
            interval: Time interval used
            result_path: Path to result JSON file
        """
        # Load result using the new model class
        benchmark_result = BenchmarkResult.load_from_file(result_path)
        summary = benchmark_result.summary
        
        row = {
            "dataset": dataset.name,
            "query_id": query_group.id,
            "engine": engine,
            "mode": summary.mode,
            "threads": summary.threads,
            "repeat": summary.repeat,
            "warmups": summary.warmups,
            "interval": interval,
            "mean_wall_s": summary.mean_wall_time_seconds,
            "p50_wall_s": summary.p50_wall_time_seconds,
            "p99_wall_s": summary.p99_wall_time_seconds,
            "mean_ttfr_s": summary.mean_ttfr_seconds,
            "mean_cpu_pct": summary.mean_cpu_avg_percent,
            "mean_rss_mb": (summary.mean_peak_rss_bytes_true or 0) / BYTES_PER_MB,
            "mean_rows": summary.mean_rows_returned,
        }
        
        self.rows.append(row)
    
    def export_manifest(self) -> Path:
        """
        Export all results to CSV manifest file.
        
        Returns:
            Path to exported manifest file
        """
        df = pd.DataFrame(self.rows)
        
        # Calculate derived metrics
        df["rows_per_sec"] = df["mean_rows"] / df["mean_wall_s"]
        df["ttfr_share_pct"] = PERCENT_MULTIPLIER * df["mean_ttfr_s"] / df["mean_wall_s"]
        
        manifest_path = self.results_dir / "manifest.csv"
        df.to_csv(manifest_path, index=False)
        
        return manifest_path


def main() -> None:
    """
    Main entry point for benchmark experiment execution.
    
    Orchestrates the complete benchmark workflow:
    1. Load configuration
    2. Run pilot tests for all combinations to determine intervals
    3. Choose unified intervals per (dataset, query_group)
    4. Execute full benchmark runs with unified intervals
    5. Collect and export results to CSV manifest
    """
    # Initialize components
    config = BenchmarkConfig.load(HERE / "config.yaml")
    results_dir = HERE / "results"
    runner = BenchmarkRunner(config, results_dir)
    processor = ResultsProcessor(results_dir)
    
    # Phase 1: Run all pilot tests and collect intervals
    print("[INFO] Phase 1: Running pilot tests for all combinations")
    pilot_intervals = {}  # {(dataset_name, query_id): {engine: interval}}
    
    for dataset in config.datasets:
        for query_group in config.query_groups:
            dataset_query_key = (dataset.name, query_group.id)
            pilot_intervals[dataset_query_key] = {}
            
            print(f"[INFO] Running pilots for {dataset.name} - {query_group.id}")
            
            for engine in config.engines:
                print(f"[INFO] Pilot test: {dataset.name} - {query_group.id} - {engine}")
                interval = runner.run_pilot(engine, dataset, query_group)
                pilot_intervals[dataset_query_key][engine] = interval
    
    # Phase 2: Choose unified intervals per (dataset, query_group)
    print("[INFO] Phase 2: Determining unified intervals")
    unified_intervals = {}  # {(dataset_name, query_id): unified_interval}
    
    for dataset_query_key, engine_intervals in pilot_intervals.items():
        intervals_list = list(engine_intervals.values())
        unified_interval = runner.choose_unified_interval(intervals_list)
        unified_intervals[dataset_query_key] = unified_interval
        
        dataset_name, query_id = dataset_query_key
        print(f"[INFO] Unified interval for {dataset_name} - {query_id}: {unified_interval:.6f}")
    
    # Phase 3: Run full benchmarks with unified intervals
    print("[INFO] Phase 3: Running full benchmarks with unified intervals")
    
    for dataset in config.datasets:
        for query_group in config.query_groups:
            dataset_query_key = (dataset.name, query_group.id)
            unified_interval = unified_intervals[dataset_query_key]
            
            for engine in config.engines:
                print(f"[INFO] Full benchmark: {dataset.name} - {query_group.id} - {engine} (interval={unified_interval:.6f})")
                
                # Run full benchmark with unified interval
                result_path = runner.run_full_benchmark(engine, dataset, query_group, unified_interval)
                
                # Collect result
                processor.add_result(dataset, query_group, engine, unified_interval, result_path)
    
    # Export results manifest
    manifest_path = processor.export_manifest()
    print(f"[DONE] wrote {manifest_path}")

if __name__ == "__main__":
    main()
