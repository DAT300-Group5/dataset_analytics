"""
DuckDB Benchmark Implementation
"""

import sys
import subprocess
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).parent))
from interfaces.benchmark import DatabaseBenchmark, BenchmarkResult, QueryResult

# Import shared CPU monitor from utils
from utils.cpu_monitor import CPUMonitor

# Import DuckDB-specific modules dynamically
import importlib.util
duckdb_dir = Path(__file__).parent / "duckdb"

# Load log_parser module
log_parser_spec = importlib.util.spec_from_file_location("duckdb_log_parser", duckdb_dir / "log_parser.py")
log_parser_module = importlib.util.module_from_spec(log_parser_spec)
log_parser_spec.loader.exec_module(log_parser_module)
DuckDBProfileParser = log_parser_module.DuckDBProfileParser


class DuckDBBenchmark(DatabaseBenchmark):
    """DuckDB benchmark implementation"""
    
    def __init__(self, 
                 db_file: str,
                 sql_file: str,
                 duckdb_cmd: str = "duckdb"):
        """
        Initialize DuckDB benchmark.
        
        Args:
            db_file: Path to database file
            sql_file: Path to SQL script
            duckdb_cmd: Path to duckdb command
        """
        super().__init__(db_file, sql_file)
        self.duckdb_cmd = duckdb_cmd
        # Results will be in duckdb/results/
        self.results_dir = Path(__file__).parent / "duckdb" / "results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
    def get_engine_name(self) -> str:
        """Get the database engine name"""
        return "DuckDB"
    
    def run(self) -> BenchmarkResult:
        """
        Execute DuckDB benchmark and return results.
        
        Returns:
            BenchmarkResult with all metrics
        """
        # Clean up old profiling files
        for old_file in self.results_dir.glob("profiling_query_*.json"):
            old_file.unlink()
        
        # Execute SQL
        peak_cpu = self._execute_sql()
        
        # Parse all profiling files
        profiling_files = sorted(self.results_dir.glob("profiling_query_*.json"))
        
        queries = []
        for profiling_file in profiling_files:
            parser = DuckDBProfileParser(str(profiling_file))
            parser.read_json()
            parser.parse_all()
            
            for query in parser.queries:
                if query.timing and query.timing.wall_time is not None:
                    wall_time = query.timing.wall_time
                    memory_bytes = query.memory.memory_used if query.memory else 0
                    output_rows = query.output_rows or 0
                    throughput = output_rows / wall_time if wall_time > 0 else 0
                    
                    queries.append(QueryResult(
                        query_number=len(queries) + 1,
                        query_sql=query.query_description or "",
                        wall_time=wall_time,
                        peak_memory_bytes=memory_bytes,
                        output_rows=output_rows,
                        throughput=throughput
                    ))
        
        # Calculate totals
        total_wall_time = sum(q.wall_time for q in queries)
        peak_memory = max((q.peak_memory_bytes for q in queries), default=0)
        total_output_rows = sum(q.output_rows for q in queries)
        overall_throughput = total_output_rows / total_wall_time if total_wall_time > 0 else 0
        
        return BenchmarkResult(
            engine_name=self.get_engine_name(),
            db_file=str(self.db_file),
            sql_file=str(self.sql_file),
            queries=queries,
            total_wall_time=total_wall_time,
            peak_memory_bytes=peak_memory,
            total_output_rows=total_output_rows,
            overall_throughput=overall_throughput,
            peak_cpu_percent=peak_cpu
        )
    
    def _execute_sql(self) -> Optional[float]:
        """
        Execute SQL file using DuckDB.
        
        Returns:
            Peak CPU percent if monitoring enabled, None otherwise
        """
        import os
        
        # Change to duckdb directory to execute (for relative paths in SQL)
        original_dir = Path.cwd()
        duckdb_dir = Path(__file__).parent / "duckdb"
        
        try:
            os.chdir(duckdb_dir)
            
            # Use absolute path for database
            db_path = original_dir / self.db_file if not Path(self.db_file).is_absolute() else self.db_file
            sql_path = original_dir / self.sql_file if not Path(self.sql_file).is_absolute() else self.sql_file
            
            command = [self.duckdb_cmd, str(db_path)]
            
            with open(sql_path, 'r') as sql_input:
                process = subprocess.Popen(
                    command,
                    stdin=sql_input,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                # Start CPU monitoring (always enabled)
                cpu_result = None
                try:
                    monitor = CPUMonitor(process.pid, interval=0.1)
                    monitor.start()
                except Exception:
                    pass
                
                # Wait for completion
                stdout, stderr = process.communicate()
                
                # Stop CPU monitoring
                try:
                    result = monitor.stop()
                    if result:
                        cpu_result = result.peak_cpu_percent
                except Exception:
                    pass
                
                if process.returncode != 0:
                    raise RuntimeError(f"DuckDB execution failed: {stderr}")
                
                return cpu_result
                
        finally:
            # Restore original directory
            os.chdir(original_dir)
