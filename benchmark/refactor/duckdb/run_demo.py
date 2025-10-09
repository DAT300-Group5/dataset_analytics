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
from pathlib import Path
from typing import Dict, Optional

# Import the log parser module
from log_parser import parse_duckdb_profile, DuckDBProfileParser

# Import process monitor
try:
    from process_monitor import ProcessMonitor
    PROCESS_MONITOR_AVAILABLE = True
except ImportError:
    PROCESS_MONITOR_AVAILABLE = False
    print("⚠ Warning: psutil not installed, process monitoring disabled")

# Try to import config, use defaults if not available
try:
    import config
    DEFAULT_DUCKDB_CMD = config.DUCKDB_CMD
    DEFAULT_SQL_FILE = config.SQL_FILE
    DEFAULT_DB_FILE = config.DB_FILE
    DEFAULT_PROFILING_OUTPUT = config.PROFILING_OUTPUT
    DEFAULT_JSON_OUTPUT = config.JSON_OUTPUT
except (ImportError, AttributeError):
    DEFAULT_DUCKDB_CMD = "duckdb"
    DEFAULT_SQL_FILE = "demo.sql"
    DEFAULT_DB_FILE = "demo.db"
    DEFAULT_PROFILING_OUTPUT = "profiling_output.json"
    DEFAULT_JSON_OUTPUT = "results.json"


class DuckDBRunner:
    """Class to manage DuckDB execution and result parsing"""
    
    def __init__(self, 
                 sql_file: str = "demo.sql",
                 db_file: str = "demo.db",
                 profiling_output: str = "profiling_output.json",
                 duckdb_cmd: str = "duckdb",
                 cpu_sample_interval: float = 0.1):
        """
        Initialize the DuckDB runner.
        
        Args:
            sql_file: Path to the SQL script to execute
            db_file: Path to the DuckDB database file
            profiling_output: Path to the profiling JSON output
            duckdb_cmd: DuckDB command (default: "duckdb")
            cpu_sample_interval: Process sampling interval in seconds (default: 0.1)
        """
        self.sql_file = Path(sql_file)
        self.db_file = Path(db_file)
        self.profiling_output = Path(profiling_output)
        self.duckdb_cmd = duckdb_cmd
        self.cpu_sample_interval = cpu_sample_interval
        self.execution_result = None
        self.cpu_result = None
        
    def check_duckdb_installed(self) -> bool:
        """
        Check if DuckDB is installed and accessible.
        
        Returns:
            True if DuckDB is available, False otherwise
        """
        try:
            result = subprocess.run(
                [self.duckdb_cmd, "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                print(f"✓ DuckDB found: {result.stdout.strip()}")
                return True
            return False
        except (subprocess.SubprocessError, FileNotFoundError):
            print(f"✗ DuckDB not found or not accessible")
            return False
    
    def prepare_environment(self):
        """Prepare the environment for execution"""
        # Remove old database and profiling files
        if self.db_file.exists():
            print(f"Removing old database: {self.db_file}")
            self.db_file.unlink()
        
        if self.profiling_output.exists():
            print(f"Removing old profiling output: {self.profiling_output}")
            self.profiling_output.unlink()
        
        # Remove old profiling query files in results directory
        import glob
        results_dir = Path("results")
        if results_dir.exists():
            for old_file in glob.glob("results/profiling_query_*.json"):
                Path(old_file).unlink()
                print(f"Removing old profiling file: {old_file}")
        
        # Also clean up any old profiling files in current directory
        for old_file in glob.glob("profiling_query_*.json"):
            Path(old_file).unlink()
            print(f"Removing old profiling file: {old_file}")
        
        # Check if SQL file exists
        if not self.sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {self.sql_file}")
    
    def execute_sql(self) -> Dict:
        """
        Execute the SQL script using duckdb command.
        
        Returns:
            Dictionary containing execution information
            
        Raises:
            subprocess.SubprocessError: If execution fails
        """
        print(f"\n{'='*60}")
        print(f"Executing SQL script: {self.sql_file}")
        print(f"Database: {self.db_file}")
        print(f"Profiling output: results/profiling_query_*.json")
        print(f"Process monitoring: Enabled (sampling every {self.cpu_sample_interval}s)")
        print(f"{'='*60}\n")
        
        # Create results directory
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)

        try:
            # Execute: duckdb demo.db < demo.sql
            with open(self.sql_file, 'r') as sql_input:
                # Start process with Popen to get PID for monitoring
                process = subprocess.Popen(
                    [self.duckdb_cmd, str(self.db_file)],
                    stdin=sql_input,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                # Start process monitoring if enabled
                cpu_monitor = ProcessMonitor(process.pid, interval=self.cpu_sample_interval)
                cpu_monitor.start()
                print(f"✓ Process monitoring started for PID {process.pid}")

                # Wait for process to complete
                stdout, stderr = process.communicate(timeout=300)
                returncode = process.returncode
                
                # Stop process monitoring
                self.cpu_result = cpu_monitor.stop()
                if self.cpu_result:
                    print(f"✓ Process monitoring completed ({self.cpu_result.samples_count} samples)")
            
            self.execution_result = {
                'returncode': returncode,
                'stdout': stdout,
                'stderr': stderr,
            }
            
            if returncode == 0:
                print(f"✓ SQL execution completed successfully")
            else:
                print(f"✗ SQL execution failed with return code: {returncode}")
                if stderr:
                    print(f"  Error output:\n{stderr}")
            
            # Check if profiling files were created
            import glob
            profiling_files = glob.glob("results/profiling_query_*.json")
            if profiling_files:
                print(f"✓ Profiling files created: {len(profiling_files)} file(s)")
            else:
                print(f"⚠ Warning: No profiling files created")
            
            return self.execution_result
            
        except subprocess.TimeoutExpired:
            print(f"✗ Execution timed out after 300 seconds")
            raise
        except Exception as e:
            print(f"✗ Execution failed with error: {e}")
            raise
    
    def parse_results(self) -> Optional[Dict]:
        """
        Parse the profiling JSON output to extract metrics.
        
        Returns:
            Dictionary containing parsed metrics or None if parsing fails
        """
        import glob
        
        # Look for all profiling_query_*.json files in results directory
        profiling_pattern = "results/profiling_query_*.json"
        profiling_files = sorted(glob.glob(profiling_pattern))
        
        if not profiling_files:
            # Try current directory as fallback
            profiling_files = sorted(glob.glob("profiling_query_*.json"))
            if not profiling_files and self.profiling_output.exists():
                profiling_files = [str(self.profiling_output)]
        
        if not profiling_files:
            print(f"✗ Cannot parse results: no profiling files found")
            return None
        
        try:
            print(f"\n{'='*60}")
            print(f"Parsing profiling output: {len(profiling_files)} file(s)")
            print(f"{'='*60}\n")
            
            all_queries = []
            
            for idx, profiling_file in enumerate(profiling_files, 1):
                try:
                    parser = DuckDBProfileParser(profiling_file)
                    parser.read_json()
                    parser.parse_all()
                    
                    # Add queries from this file
                    for query in parser.queries:
                        query.query_number = len(all_queries) + 1
                        all_queries.append(query)
                        
                except Exception as e:
                    print(f"⚠ Warning: Error parsing {profiling_file}: {e}")
                    continue
            
            if not all_queries:
                print(f"✗ No profiling data could be parsed")
                return None
            
            # Create a temporary parser to use its export methods
            temp_parser = DuckDBProfileParser(profiling_files[0])
            temp_parser.queries = all_queries
            results = temp_parser.export_to_dict()
            
            # Print summary
            summary = results.get('summary', {})
            print(f"Summary:")
            print(f"  Total queries: {summary.get('total_queries', 0)}")
            
            timing = summary.get('timing', {})
            if timing:
                print(f"\nTiming:")
                print(f"  Total wall time: {timing.get('total_wall_time', 0):.4f} seconds")
                print(f"  Average wall time: {timing.get('avg_wall_time', 0):.4f} seconds")
                print(f"  Min wall time: {timing.get('min_wall_time', 0):.4f} seconds")
                print(f"  Max wall time: {timing.get('max_wall_time', 0):.4f} seconds")
                
                # DuckDB profiling also includes CPU time if available
                if timing.get('total_cpu_time'):
                    print(f"  Total CPU time: {timing.get('total_cpu_time', 0):.4f} seconds")
                if timing.get('avg_cpu_time'):
                    print(f"  Average CPU time: {timing.get('avg_cpu_time', 0):.4f} seconds")
            
            memory = summary.get('memory', {})
            if memory and memory.get('peak_memory_kb', 0) > 0:
                print(f"\nMemory:")
                avg_mem_kb = memory.get('avg_memory_kb', 0)
                peak_mem_kb = memory.get('peak_memory_kb', 0)
                
                if avg_mem_kb > 0:
                    print(f"  Average memory used: {avg_mem_kb*1024:.0f} bytes ({avg_mem_kb:.2f} KB)")
                print(f"  Peak memory used: {peak_mem_kb*1024:.0f} bytes ({peak_mem_kb:.2f} KB)")
                
                # Show MB conversion if memory is significant
                if peak_mem_kb > 1024:
                    print(f"  Peak memory used (MB): {peak_mem_kb/1024:.2f} MB")
            
            # Print throughput statistics
            throughput = summary.get('throughput', {})
            if throughput and throughput.get('total_output_rows', 0) > 0:
                print(f"\nThroughput:")
                print(f"  Total output rows: {throughput.get('total_output_rows', 0)}")
                print(f"  Overall throughput: {throughput.get('overall_throughput_rows_per_sec', 0):.2f} rows/sec")
                
                last_query_rows = throughput.get('last_query_rows', 0)
                last_query_time = throughput.get('last_query_time', 0)
                last_query_throughput = throughput.get('last_query_throughput_rows_per_sec', 0)
                
                if last_query_rows > 0:
                    print(f"\n  Last query performance:")
                    print(f"    Output rows: {last_query_rows}")
                    print(f"    Execution time: {last_query_time:.4f} seconds")
                    print(f"    Throughput: {last_query_throughput:.2f} rows/sec")
            
            # Print process monitoring results if available
            if self.cpu_result:
                print(f"\nProcess Resource Usage (sampled):")
                print(f"  Process duration: {self.cpu_result.process_duration_seconds:.4f} seconds")
                print(f"  Peak CPU: {self.cpu_result.peak_cpu_percent:.2f}%")
                print(f"  Average CPU: {self.cpu_result.avg_cpu_percent:.2f}%")
                print(f"  Min CPU: {self.cpu_result.min_cpu_percent:.2f}%")
                print(f"  Samples collected: {self.cpu_result.samples_count}")
                print(f"  Peak memory (RSS): {self.cpu_result.peak_memory_mb:.2f} MB")
            
            return results
            
        except Exception as e:
            print(f"✗ Error parsing results: {e}")
            return None
    
    def save_results(self, results: Dict, output_file: str = "results.json"):
        """
        Save parsed results to a JSON file.
        
        Args:
            results: Dictionary containing parsed results
            output_file: Path to the output JSON file
        """
        output_path = Path(output_file)
        
        try:
            # Combine execution info and parsed results
            combined = {
                'execution': self.execution_result,
                'metrics': results
            }
            
            # Add process monitoring results if available
            if self.cpu_result:
                combined['cpu_monitoring'] = self.cpu_result.to_dict()
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(combined, f, indent=2, ensure_ascii=False)
            
            print(f"\n✓ Results saved to: {output_path}")
            
        except Exception as e:
            print(f"✗ Error saving results: {e}")
    
    def run(self, save_json: bool = True, json_output: str = "results.json") -> Dict:
        """
        Run the complete workflow: prepare, execute, parse, and save.
        
        Args:
            save_json: Whether to save results to JSON file
            json_output: Path to the output JSON file
            
        Returns:
            Dictionary containing all results
        """
        try:
            # Check environment
            if not self.check_duckdb_installed():
                raise RuntimeError("DuckDB is not installed or not accessible")
            
            # Prepare
            self.prepare_environment()
            
            # Execute SQL
            execution_info = self.execute_sql()
            
            # Parse results
            parsed_results = self.parse_results()
            
            # Save results
            if save_json and parsed_results:
                self.save_results(parsed_results, json_output)
            
            return {
                'success': execution_info['returncode'] == 0,
                'execution': execution_info,
                'metrics': parsed_results
            }
            
        except Exception as e:
            print(f"\n✗ Error during execution: {e}")
            return {
                'success': False,
                'error': str(e)
            }


def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(
        description="Execute DuckDB SQL script and parse performance metrics",
        epilog="Note: Command line arguments override config.py settings"
    )
    parser.add_argument(
        "--sql-file",
        default=DEFAULT_SQL_FILE,
        help=f"Path to the SQL script file (default: {DEFAULT_SQL_FILE})"
    )
    parser.add_argument(
        "--db-file",
        default=DEFAULT_DB_FILE,
        help=f"Path to the DuckDB database file (default: {DEFAULT_DB_FILE})"
    )
    parser.add_argument(
        "--profiling-output",
        default=DEFAULT_PROFILING_OUTPUT,
        help=f"Path to the profiling output file (default: {DEFAULT_PROFILING_OUTPUT})"
    )
    parser.add_argument(
        "--json-output",
        default=DEFAULT_JSON_OUTPUT,
        help=f"Path to save JSON results (default: {DEFAULT_JSON_OUTPUT})"
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save results to JSON file"
    )
    parser.add_argument(
        "--duckdb-cmd",
        default=DEFAULT_DUCKDB_CMD,
        help=f"DuckDB command or full path (default: {DEFAULT_DUCKDB_CMD})"
    )
    parser.add_argument(
        "--no-cpu-monitor",
        action="store_true",
        help="Disable process monitoring"
    )
    parser.add_argument(
        "--cpu-interval",
        type=float,
        default=0.1,
        help="Process sampling interval in seconds (default: 0.1)"
    )
    
    args = parser.parse_args()
    
    # Create runner
    runner = DuckDBRunner(
        sql_file=args.sql_file,
        db_file=args.db_file,
        profiling_output=args.profiling_output,
        duckdb_cmd=args.duckdb_cmd,
        cpu_sample_interval=args.cpu_interval
    )
    
    # Run
    results = runner.run(
        save_json=not args.no_save,
        json_output=args.json_output
    )
    
    # Exit with appropriate code
    sys.exit(0 if results.get('success', False) else 1)


if __name__ == "__main__":
    main()
