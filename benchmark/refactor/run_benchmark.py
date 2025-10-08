#!/usr/bin/env python3
"""
Unified Benchmark Runner

This script provides a unified interface to run benchmarks on SQLite and DuckDB.
"""

import sys
import json
import argparse
from pathlib import Path
from typing import Any, Dict
from tabulate import tabulate

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from interfaces.benchmark import BenchmarkResult


def print_results(result: BenchmarkResult):
    """Print benchmark results in a formatted way - showing only 5 core metrics"""
    print("\n" + "=" * 70)
    print(f"=== {result.engine_name} Benchmark Results ===")
    print("=" * 70)
    
    print(f"\n📁 Database: {result.db_file}")
    print(f"📄 SQL File: {result.sql_file}")
    print(f"📊 Total Queries: {len(result.queries)}")
    
    print(f"\n🎯 5 Core Metrics:")
    print(f"  1. Wall Time:            {result.total_wall_time:.6f} seconds")
    print(f"  2. Peak Memory:          {result.peak_memory_bytes / (1024*1024):.2f} MB")
    print(f"  3. Total Output Rows:    {result.total_output_rows}")
    print(f"  4. Overall Throughput:   {result.overall_throughput:.2f} rows/sec")
    print(f"  5. Peak CPU Usage:       {result.peak_cpu_percent:.2f}%")
    
    print("\n" + "=" * 70)


def print_comparison_results(sqlite_result: BenchmarkResult, duckdb_result: BenchmarkResult):
    """Print side-by-side comparison of SQLite and DuckDB results"""
    print("\n🎯 5 Core Metrics:")
    
    # Prepare table data
    headers = ["", "SQLite", "DuckDB"]
    table_data = [
        ["1. Wall Time", f"{sqlite_result.total_wall_time:.6f} s", f"{duckdb_result.total_wall_time:.6f} s"],
        ["2. Peak Memory", f"{sqlite_result.peak_memory_bytes / (1024*1024):.2f} MB", f"{duckdb_result.peak_memory_bytes / (1024*1024):.2f} MB"],
        ["3. Total Rows", f"{sqlite_result.total_output_rows}", f"{duckdb_result.total_output_rows}"],
        ["4. Throughput", f"{sqlite_result.overall_throughput:.2f} r/s", f"{duckdb_result.overall_throughput:.2f} r/s"],
        ["5. Peak CPU", f"{sqlite_result.peak_cpu_percent:.2f}%", f"{duckdb_result.peak_cpu_percent:.2f}%"]
    ]
    
    # Print table with heavy_grid style
    print(tabulate(table_data, headers=headers, tablefmt="heavy_grid", stralign="right", numalign="right"))
    
    # Calculate comparison ratios
    time_ratio = duckdb_result.total_wall_time / sqlite_result.total_wall_time
    memory_ratio = duckdb_result.peak_memory_bytes / sqlite_result.peak_memory_bytes
    throughput_ratio = duckdb_result.overall_throughput / sqlite_result.overall_throughput
    faster_engine = 'DuckDB' if duckdb_result.total_wall_time < sqlite_result.total_wall_time else 'SQLite'
    
    print(f"\n📊 Comparison:")
    if faster_engine == 'DuckDB':
        print(f"  ⚡ DuckDB is {1/time_ratio:.2f}x faster (wall time)")
    else:
        print(f"  ⚡ SQLite is {time_ratio:.2f}x faster (wall time)")
    print(f"  🧠 DuckDB uses {memory_ratio:.2f}x {'more' if memory_ratio > 1 else 'less'} memory")
    print(f"  🚀 DuckDB has {throughput_ratio:.2f}x {'higher' if throughput_ratio > 1 else 'lower'} throughput")


def main():
    parser = argparse.ArgumentParser(
        description="Run database benchmarks with unified interface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run SQLite benchmark
  python3 run_benchmark.py --engine sqlite --db test.db --sql demo.sql
  
  # Run DuckDB benchmark
  python3 run_benchmark.py --engine duckdb --db test.db --sql demo.sql
  
  # Run both and compare
  python3 run_benchmark.py --engine both --db test.db --sql demo.sql
  
  # Save results to JSON
  python3 run_benchmark.py --engine sqlite --db test.db --sql demo.sql --output results.json
        """
    )
    
    parser.add_argument(
        '--engine',
        choices=['sqlite', 'duckdb', 'both'],
        required=True,
        help='Database engine to benchmark'
    )
    
    parser.add_argument(
        '--db',
        required=True,
        help='Path to database file (will be created if doesn\'t exist)'
    )
    
    parser.add_argument(
        '--sql',
        help='Path to SQL file to execute (for single engine or both)'
    )
    
    parser.add_argument(
        '--sqlite-sql',
        help='Path to SQLite SQL file (for comparison mode)'
    )
    
    parser.add_argument(
        '--duckdb-sql',
        help='Path to DuckDB SQL file (for comparison mode)'
    )
    
    parser.add_argument(
        '--output',
        help='Path to save JSON results (optional)'
    )
    
    parser.add_argument(
        '--sqlite-cmd',
        default='sqlite3',
        help='Path to sqlite3 command (default: sqlite3)'
    )
    
    parser.add_argument(
        '--duckdb-cmd',
        default='duckdb',
        help='Path to duckdb command (default: duckdb)'
    )
    
    args = parser.parse_args()
    
    # Validate SQL file arguments
    if args.engine == 'both':
        if not args.sqlite_sql or not args.duckdb_sql:
            if not args.sql:
                print("❌ Error: For comparison mode (--engine both), either provide:")
                print("  - --sql (uses same SQL for both engines)")
                print("  - --sqlite-sql and --duckdb-sql (different SQL files)")
                return 1
            args.sqlite_sql = args.sql
            args.duckdb_sql = args.sql
    else:
        if not args.sql:
            print("❌ Error: --sql is required")
            return 1
        args.sqlite_sql = args.sql
        args.duckdb_sql = args.sql
    
    results = []
    temp_db_files = []  # Track temporary database files for cleanup
    
    # Run SQLite benchmark
    if args.engine in ['sqlite', 'both']:
        print(f"\n🔵 Running SQLite benchmark...")
        try:
            # Import here to pass custom command
            from sqlite_benchmark import SQLiteBenchmark
            db_name = args.db if args.engine == 'sqlite' else f"sqlite_{args.db}"
            benchmark = SQLiteBenchmark(
                db_name,
                args.sqlite_sql,
                args.sqlite_cmd
            )
            result = benchmark.run()
            # Only print individual results in single-engine mode
            if args.engine != 'both':
                print_results(result)
            results.append(result)
            
            # Track temp file for cleanup (only in comparison mode)
            if args.engine == 'both':
                temp_db_files.append(db_name)
        except Exception as e:
            print(f"❌ SQLite benchmark failed: {e}")
            if args.engine == 'sqlite':
                return 1
    
    # Run DuckDB benchmark
    if args.engine in ['duckdb', 'both']:
        print(f"\n🦆 Running DuckDB benchmark...")
        try:
            # Import here to pass custom command
            from duckdb_benchmark import DuckDBBenchmark
            db_name = args.db if args.engine == 'duckdb' else f"duckdb_{args.db}"
            benchmark = DuckDBBenchmark(
                db_name,
                args.duckdb_sql,
                args.duckdb_cmd
            )
            result = benchmark.run()
            # Only print individual results in single-engine mode
            if args.engine != 'both':
                print_results(result)
            results.append(result)
            
            # Track temp file for cleanup (only in comparison mode)
            if args.engine == 'both':
                temp_db_files.append(db_name)
        except Exception as e:
            print(f"❌ DuckDB benchmark failed: {e}")
            if args.engine == 'duckdb':
                return 1
    
    # Print comparison results if both engines were run
    if args.engine == 'both' and len(results) == 2:
        print_comparison_results(results[0], results[1])
    
    # Save results to JSON if requested
    if args.output:
        output_data: Dict[str, Any] = {
            'results': [r.to_dict() for r in results]
        }
        
        # Add comparison if both engines were run
        if len(results) == 2:
            sqlite_result = results[0]
            duckdb_result = results[1]
            comparison = {
                'wall_time_ratio': duckdb_result.total_wall_time / sqlite_result.total_wall_time,
                'memory_ratio': duckdb_result.peak_memory_bytes / sqlite_result.peak_memory_bytes,
                'throughput_ratio': duckdb_result.overall_throughput / sqlite_result.overall_throughput,
                'faster_engine': 'DuckDB' if duckdb_result.total_wall_time < sqlite_result.total_wall_time else 'SQLite'
            }
            output_data['comparison'] = comparison
        
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\n💾 Results saved to: {args.output}")
    
    # Clean up temporary database files (only in comparison mode)
    if temp_db_files:
        print(f"\n🧹 Cleaning up temporary database files...")
        for db_file in temp_db_files:
            db_path = Path(db_file)
            if db_path.exists():
                try:
                    db_path.unlink()
                    print(f"  ✓ Deleted: {db_file}")
                except Exception as e:
                    print(f"  ✗ Failed to delete {db_file}: {e}")
            
            # Also clean up DuckDB's .wal files
            wal_file = Path(f"{db_file}.wal")
            if wal_file.exists():
                try:
                    wal_file.unlink()
                    print(f"  ✓ Deleted: {db_file}.wal")
                except Exception:
                    pass
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
