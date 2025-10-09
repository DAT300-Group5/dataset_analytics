#!/usr/bin/env python3
"""
SQLite Demo Runner

This script executes a SQLite demo SQL file using a subprocess and parses
the resulting output log to extract performance metrics.
"""

import sys
import subprocess
import json
import argparse
from pathlib import Path
from typing import Dict, Optional

# Import the log parser module
from log_parser import parse_sqlite_log

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
    DEFAULT_SQLITE_CMD = config.SQLITE_CMD
    DEFAULT_SQL_FILE = config.SQL_FILE
    DEFAULT_DB_FILE = config.DB_FILE
    DEFAULT_OUTPUT_LOG = config.OUTPUT_LOG
    DEFAULT_JSON_OUTPUT = config.JSON_OUTPUT
except (ImportError, AttributeError):
    DEFAULT_SQLITE_CMD = "sqlite3"
    DEFAULT_SQL_FILE = "demo.sql"
    DEFAULT_DB_FILE = "demo.db"
    DEFAULT_OUTPUT_LOG = "output.log"
    DEFAULT_JSON_OUTPUT = "results.json"


class SQLiteRunner:
    """Class to manage SQLite execution and result parsing"""
    
    def __init__(self, 
                 sql_file: str = "demo.sql",
                 db_file: str = "demo.db",
                 output_log: str = "output.log",
                 sqlite_cmd: str = "sqlite3",
                 cpu_sample_interval: float = 0.1):
        """
        Initialize the SQLite runner.
        
        Args:
            sql_file: Path to the SQL script to execute
            db_file: Path to the SQLite database file
            output_log: Path to the output log file
            sqlite_cmd: SQLite3 command (default: "sqlite3")
            cpu_sample_interval: Process sampling interval in seconds (default: 0.1)
        """
        self.sql_file = Path(sql_file)
        self.db_file = Path(db_file)
        self.output_log = Path(output_log)
        self.sqlite_cmd = sqlite_cmd
        self.cpu_sample_interval = cpu_sample_interval
        self.execution_result = None
        self.cpu_result = None
        
    def check_sqlite_installed(self) -> bool:
        """
        Check if SQLite3 is installed and accessible.
        
        Returns:
            True if SQLite3 is available, False otherwise
        """
        try:
            result = subprocess.run(
                [self.sqlite_cmd, "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                print(f"✓ SQLite3 found: {result.stdout.strip()}")
                return True
            return False
        except (subprocess.SubprocessError, FileNotFoundError):
            print(f"✗ SQLite3 not found or not accessible")
            return False
    
    def prepare_environment(self):
        """Prepare the environment for execution"""
        # Remove old database and log files
        if self.db_file.exists():
            print(f"Removing old database: {self.db_file}")
            self.db_file.unlink()
        
        if self.output_log.exists():
            print(f"Removing old log file: {self.output_log}")
            self.output_log.unlink()
        
        # Check if SQL file exists
        if not self.sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {self.sql_file}")
    
    def execute_sql(self) -> Dict:
        """
        Execute the SQL script using sqlite3 command.
        
        Returns:
            Dictionary containing execution information
            
        Raises:
            subprocess.SubprocessError: If execution fails
        """
        print(f"\n{'='*60}")
        print(f"Executing SQL script: {self.sql_file}")
        print(f"Database: {self.db_file}")
        print(f"Output log: {self.output_log}")
        print(f"Process monitoring: Enabled (sampling every {self.cpu_sample_interval}s)")
        print(f"{'='*60}\n")

        try:
            # Execute: sqlite3 demo.db < demo.sql
            with open(self.sql_file, 'r') as sql_input:
                # Start process with Popen to get PID for monitoring
                process = subprocess.Popen(
                    [self.sqlite_cmd, str(self.db_file)],
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
            
            # Check if output log was created
            if self.output_log.exists():
                log_size = self.output_log.stat().st_size
                print(f"✓ Output log created: {self.output_log} ({log_size} bytes)")
            else:
                print(f"⚠ Warning: Output log not created")
            
            return self.execution_result
            
        except subprocess.TimeoutExpired:
            print(f"✗ Execution timed out after 300 seconds")
            raise
        except Exception as e:
            print(f"✗ Execution failed with error: {e}")
            raise
    
    def parse_results(self) -> Optional[Dict]:
        """
        Parse the output log file to extract metrics.
        
        Returns:
            Dictionary containing parsed metrics or None if parsing fails
        """
        if not self.output_log.exists():
            print(f"✗ Cannot parse results: log file not found")
            return None
        
        try:
            print(f"\n{'='*60}")
            print(f"Parsing output log: {self.output_log}")
            print(f"{'='*60}\n")
            
            results = parse_sqlite_log(str(self.output_log))
            
            # Print summary
            summary = results.get('summary', {})
            print(f"Summary:")
            print(f"  Total queries: {summary.get('total_queries', 0)}")
            
            timing = summary.get('timing', {})
            if timing:
                print(f"\nTiming:")
                print(f"  Total run time: {timing.get('total_run_time', 0):.4f} seconds")
                print(f"  Average run time: {timing.get('avg_run_time', 0):.4f} seconds")
                print(f"  Min run time: {timing.get('min_run_time', 0):.4f} seconds")
                print(f"  Max run time: {timing.get('max_run_time', 0):.4f} seconds")
                print(f"  Total user time: {timing.get('total_user_time', 0):.4f} seconds")
                print(f"  Total system time: {timing.get('total_system_time', 0):.4f} seconds")
            
            memory = summary.get('memory', {})
            if memory and memory.get('avg_memory_used', 0) > 0:
                print(f"\nMemory:")
                print(f"  Average memory used: {memory.get('avg_memory_used', 0):.0f} bytes ({memory.get('avg_memory_used', 0)/1024:.2f} KB)")
                print(f"  Max memory used: {memory.get('max_memory_used', 0):.0f} bytes ({memory.get('max_memory_used', 0)/1024:.2f} KB)")
                print(f"  Average heap usage: {memory.get('avg_heap_usage', 0):.0f} bytes ({memory.get('avg_heap_usage', 0)/1024:.2f} KB)")
                print(f"  Max heap usage: {memory.get('max_heap_usage', 0):.0f} bytes ({memory.get('max_heap_usage', 0)/1024:.2f} KB)")
            
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
            if not self.check_sqlite_installed():
                raise RuntimeError("SQLite3 is not installed or not accessible")
            
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
        description="Execute SQLite SQL script and parse performance metrics",
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
        help=f"Path to the SQLite database file (default: {DEFAULT_DB_FILE})"
    )
    parser.add_argument(
        "--output-log",
        default=DEFAULT_OUTPUT_LOG,
        help=f"Path to the output log file (default: {DEFAULT_OUTPUT_LOG})"
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
        "--sqlite-cmd",
        default=DEFAULT_SQLITE_CMD,
        help=f"SQLite3 command or full path (default: {DEFAULT_SQLITE_CMD})"
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
    runner = SQLiteRunner(
        sql_file=args.sql_file,
        db_file=args.db_file,
        output_log=args.output_log,
        sqlite_cmd=args.sqlite_cmd,
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
