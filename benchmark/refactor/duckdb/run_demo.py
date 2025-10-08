#!/usr/bin/env python3
"""
DuckDB Demo Runner

This script executes a DuckDB demo SQL file using a subprocess and parses
the resulting JSON profiling output to extract performance metrics.
"""

import sys
import subprocess
import json
import argparse
import time
from pathlib import Path
from typing import Dict, Optional

# Import the log parser module
from log_parser import parse_duckdb_profile, DuckDBProfileParser

# Import CPU monitor
try:
    from cpu_monitor import CPUMonitor
    CPU_MONITOR_AVAILABLE = True
except ImportError:
    CPU_MONITOR_AVAILABLE = False
    print("⚠ Warning: psutil not installed, CPU monitoring disabled")

# Try to import config, use defaults if not available
try:
    import config
    DEFAULT_DUCKDB_CMD = config.DUCKDB_CMD
except (ImportError, AttributeError):
    DEFAULT_DUCKDB_CMD = "duckdb"


class DuckDBRunner:
    """Class to run DuckDB SQL scripts and collect performance metrics"""
    
    def __init__(self, 
                 duckdb_cmd: str = DEFAULT_DUCKDB_CMD,
                 sql_file: str = "demo.sql",
                 db_file: str = "demo.db",
                 profiling_output: str = "profiling_output.json",
                 results_file: str = "results.json",
                 enable_cpu_monitor: bool = True):
        """
        Initialize DuckDB runner.
        
        Args:
            duckdb_cmd: Path to duckdb executable
            sql_file: Path to SQL script file
            db_file: Path to database file (will be created)
            profiling_output: Path to profiling JSON output
            results_file: Path to save final results JSON
            enable_cpu_monitor: Whether to enable CPU monitoring
        """
        self.duckdb_cmd = duckdb_cmd
        self.sql_file = sql_file
        self.db_file = db_file
        self.profiling_output = profiling_output
        self.results_file = results_file
        self.enable_cpu_monitor = enable_cpu_monitor and CPU_MONITOR_AVAILABLE
        self.cpu_result = None
        
    def execute_sql(self) -> tuple[int, str, str]:
        """
        Execute the SQL file using DuckDB CLI.
        
        Returns:
            Tuple of (return_code, stdout, stderr)
        """
        print(f"🦆 Executing DuckDB script: {self.sql_file}")
        print(f"   Using DuckDB: {self.duckdb_cmd}")
        print(f"   Database: {self.db_file}")
        print(f"   Profiling output: results/profiling_query_*.json")
        
        # Create results directory
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        
        # Remove old database and profiling output if they exist
        Path(self.db_file).unlink(missing_ok=True)
        Path(self.profiling_output).unlink(missing_ok=True)
        
        # Remove old profiling query files in results directory
        import glob
        for old_file in glob.glob("results/profiling_query_*.json"):
            Path(old_file).unlink(missing_ok=True)
        
        # Also clean up any old profiling files in current directory
        for old_file in glob.glob("profiling_query_*.json"):
            Path(old_file).unlink(missing_ok=True)
        
        start_time = time.time()
        
        # Construct command: duckdb database_file < script.sql
        command = [self.duckdb_cmd, self.db_file]
        
        try:
            # Start the subprocess
            with open(self.sql_file, 'r') as sql_input:
                process = subprocess.Popen(
                    command,
                    stdin=sql_input,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                # Start CPU monitoring if enabled
                if self.enable_cpu_monitor:
                    print("📊 CPU monitoring enabled")
                    monitor = CPUMonitor(process.pid, interval=0.1)
                    monitor.start()
                
                # Wait for process to complete
                stdout, stderr = process.communicate()
                
                # Stop CPU monitoring
                if self.enable_cpu_monitor:
                    self.cpu_result = monitor.stop()
                
                end_time = time.time()
                elapsed = end_time - start_time
                
                print(f"✅ Execution completed in {elapsed:.3f} seconds")
                
                return process.returncode, stdout, stderr
                
        except FileNotFoundError:
            print(f"❌ Error: DuckDB executable not found: {self.duckdb_cmd}")
            print("   Please install DuckDB or specify the correct path using --duckdb-cmd")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error executing DuckDB: {e}")
            sys.exit(1)
    
    def parse_results(self) -> Dict:
        """
        Parse the profiling JSON output and extract metrics.
        Handles multiple profiling files (results/profiling_query_N.json).
        
        Returns:
            Dictionary with parsed results
        """
        import glob
        
        # Look for all profiling_query_*.json files in results directory
        profiling_pattern = "results/profiling_query_*.json"
        profiling_files = sorted(glob.glob(profiling_pattern))
        
        if not profiling_files:
            print(f"⚠ Warning: No profiling files found matching {profiling_pattern}")
            # Try current directory as fallback
            profiling_pattern_fallback = "profiling_query_*.json"
            profiling_files = sorted(glob.glob(profiling_pattern_fallback))
            if not profiling_files:
                print("   Falling back to single profiling output file...")
                profiling_files = [self.profiling_output]
        
        print(f"\n📊 Parsing {len(profiling_files)} profiling file(s)")
        
        all_queries = []
        
        for idx, profiling_file in enumerate(profiling_files, 1):
            try:
                print(f"   [{idx}/{len(profiling_files)}] Parsing {profiling_file}")
                parser = DuckDBProfileParser(profiling_file)
                parser.read_json()
                parser.parse_all()
                
                # Add queries from this file
                for query in parser.queries:
                    # Override query number to maintain sequence
                    query.query_number = len(all_queries) + 1
                    all_queries.append(query)
                    
            except FileNotFoundError:
                print(f"   ⚠ Warning: File not found: {profiling_file}")
                continue
            except Exception as e:
                print(f"   ⚠ Warning: Error parsing {profiling_file}: {e}")
                continue
        
        if not all_queries:
            return {"error": "No profiling data could be parsed"}
        
        # Create a temporary parser to use its export methods
        temp_parser = DuckDBProfileParser(profiling_files[0])
        temp_parser.queries = all_queries
        results = temp_parser.export_to_dict()
        
        # Add CPU monitoring results if available
        if self.cpu_result:
            results['cpu_monitoring'] = self.cpu_result.to_dict()
        
        return results
    
    def save_results(self, results: Dict):
        """
        Save results to JSON file in results directory.
        
        Args:
            results: Dictionary to save
        """
        try:
            # Ensure results directory exists
            results_dir = Path("results")
            results_dir.mkdir(exist_ok=True)
            
            # Save to results directory
            results_path = results_dir / self.results_file
            with open(results_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)
            print(f"💾 Results saved to: {results_path}")
        except Exception as e:
            print(f"⚠ Warning: Could not save results: {e}")
    
    def display_results(self, results: Dict):
        """
        Display formatted results to console.
        
        Args:
            results: Parsed results dictionary
        """
        if 'error' in results:
            print(f"\n❌ Error: {results['error']}")
            return
        
        print("\n" + "=" * 60)
        print("=== DuckDB Performance Demo Results ===")
        print("=" * 60)
        
        summary = results.get('summary', {})
        
        print("\n📈 Overall Summary:")
        print(f"  Total Queries:        {summary.get('total_queries', 0)}")
        print(f"  Total Wall Time:      {summary.get('total_wall_time', 0):.4f} seconds")
        print(f"  Peak Memory Used:     {summary.get('peak_memory_kb', 0):.2f} KB")
        print(f"  Total Output Rows:    {summary.get('total_output_rows', 0)}")
        print(f"  Overall Throughput:   {summary.get('overall_throughput_rows_per_sec', 0):.2f} rows/sec")
        print(f"  Last Query Throughput: {summary.get('last_query_throughput_rows_per_sec', 0):.2f} rows/sec")
        
        # Display CPU monitoring results if available
        if 'cpu_monitoring' in results:
            cpu_data = results['cpu_monitoring']
            print("\n🖥️  CPU Monitoring:")
            print(f"  Peak CPU Usage:       {cpu_data.get('peak_cpu_percent', 0):.2f}%")
            print(f"  Average CPU Usage:    {cpu_data.get('avg_cpu_percent', 0):.2f}%")
            print(f"  Peak Memory (RSS):    {cpu_data.get('peak_memory_mb', 0):.2f} MB")
            print(f"  Average Memory (RSS): {cpu_data.get('avg_memory_mb', 0):.2f} MB")
            print(f"  Samples Collected:    {cpu_data.get('samples_count', 0)}")
        
        # Display per-query results
        queries = results.get('queries', [])
        if queries:
            print(f"\n📋 Per-Query Results ({len(queries)} queries):")
            print("-" * 60)
            
            for query in queries:
                qnum = query.get('query_number', '?')
                qdesc = query.get('query_description', 'N/A')
                
                # Truncate long descriptions
                if len(qdesc) > 50:
                    qdesc = qdesc[:47] + "..."
                
                print(f"\n  Query {qnum}: {qdesc}")
                
                timing = query.get('timing', {})
                if timing and timing.get('wall_time') is not None:
                    print(f"    Wall Time:     {timing.get('wall_time', 0):.6f} seconds")
                
                memory = query.get('memory', {})
                if memory and memory.get('memory_used') is not None:
                    mem_kb = memory.get('memory_used', 0) / 1024
                    print(f"    Memory Used:   {mem_kb:.2f} KB")
                
                output_rows = query.get('output_rows')
                if output_rows is not None:
                    print(f"    Output Rows:   {output_rows}")
                    
                    # Calculate query-specific throughput
                    if timing and timing.get('wall_time', 0) > 0:
                        throughput = output_rows / timing['wall_time']
                        print(f"    Throughput:    {throughput:.2f} rows/sec")
        
        print("\n" + "=" * 60)
    
    def run(self):
        """Main execution method"""
        # Execute SQL
        return_code, stdout, stderr = self.execute_sql()
        
        if return_code != 0:
            print(f"\n⚠ DuckDB exited with code {return_code}")
            if stderr:
                print(f"Error output:\n{stderr}")
        
        # Parse results
        results = self.parse_results()
        
        # Display results
        self.display_results(results)
        
        # Save results
        self.save_results(results)
        
        return results


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Run DuckDB performance demo with profiling",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default settings
  python3 run_demo.py
  
  # Use custom DuckDB binary
  python3 run_demo.py --duckdb-cmd /path/to/duckdb
  
  # Disable CPU monitoring
  python3 run_demo.py --no-cpu-monitor
  
  # Use custom SQL file
  python3 run_demo.py --sql-file my_queries.sql
        """
    )
    
    parser.add_argument(
        '--duckdb-cmd',
        default=DEFAULT_DUCKDB_CMD,
        help=f'Path to DuckDB executable (default: {DEFAULT_DUCKDB_CMD})'
    )
    
    parser.add_argument(
        '--sql-file',
        default='demo.sql',
        help='Path to SQL script file (default: demo.sql)'
    )
    
    parser.add_argument(
        '--db-file',
        default='demo.db',
        help='Path to database file (default: demo.db)'
    )
    
    parser.add_argument(
        '--profiling-output',
        default='profiling_output.json',
        help='Path to profiling JSON output (default: profiling_output.json)'
    )
    
    parser.add_argument(
        '--results-file',
        default='results.json',
        help='Path to save results JSON (default: results.json)'
    )
    
    parser.add_argument(
        '--no-cpu-monitor',
        action='store_true',
        help='Disable CPU monitoring'
    )
    
    args = parser.parse_args()
    
    # Create runner
    runner = DuckDBRunner(
        duckdb_cmd=args.duckdb_cmd,
        sql_file=args.sql_file,
        db_file=args.db_file,
        profiling_output=args.profiling_output,
        results_file=args.results_file,
        enable_cpu_monitor=not args.no_cpu_monitor
    )
    
    # Run demo
    runner.run()


if __name__ == '__main__':
    main()
