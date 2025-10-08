"""
SQLite Benchmark Implementation
"""

import sys
import subprocess
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).parent))
from interfaces.benchmark import DatabaseBenchmark, BenchmarkResult, QueryResult

# Import shared CPU monitor from utils
from utils.cpu_monitor import CPUMonitor

# Import SQLite-specific modules
sys.path.append(str(Path(__file__).parent / "sqlite"))
from log_parser import SQLiteLogParser


class SQLiteBenchmark(DatabaseBenchmark):
    """SQLite benchmark implementation"""
    
    def __init__(self, 
                 db_file: str,
                 sql_file: str,
                 sqlite_cmd: str = "sqlite3"):
        """
        Initialize SQLite benchmark.
        
        Args:
            db_file: Path to database file
            sql_file: Path to SQL script
            sqlite_cmd: Path to sqlite3 command
        """
        super().__init__(db_file, sql_file)
        self.sqlite_cmd = sqlite_cmd
        # Output log will be in sqlite/results/output.log
        self.output_log = Path(__file__).parent / "sqlite" / "results" / "output.log"
        self.output_log.parent.mkdir(parents=True, exist_ok=True)
        
    def get_engine_name(self) -> str:
        """Get the database engine name"""
        return "SQLite"
    
    def run(self) -> BenchmarkResult:
        """
        Execute SQLite benchmark and return results.
        
        Returns:
            BenchmarkResult with all metrics
        """
        # Clean up old output log
        if self.output_log.exists():
            self.output_log.unlink()
        
        # Execute SQL
        peak_cpu = self._execute_sql()
        
        # Parse results
        parser = SQLiteLogParser(str(self.output_log))
        parser.read_log()
        parser.parse_all()
        
        # Convert to QueryResult objects
        queries = []
        for query in parser.queries:
            if query.timing and query.timing.run_time is not None:
                wall_time = query.timing.run_time
                memory_bytes = query.memory.memory_used if query.memory else 0
                output_rows = query.output_rows or 0
                throughput = output_rows / wall_time if wall_time > 0 else 0
                
                queries.append(QueryResult(
                    query_number=query.query_number or 0,
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
        Execute SQL file using SQLite.
        
        Returns:
            Peak CPU percent if monitoring enabled, None otherwise
        """
        import os
        
        # Change to sqlite directory to execute (for relative paths in SQL)
        original_dir = Path.cwd()
        sqlite_dir = Path(__file__).parent / "sqlite"
        
        try:
            os.chdir(sqlite_dir)
            
            # Use absolute path for database
            db_path = original_dir / self.db_file if not Path(self.db_file).is_absolute() else self.db_file
            sql_path = original_dir / self.sql_file if not Path(self.sql_file).is_absolute() else self.sql_file
            
            command = [self.sqlite_cmd, str(db_path)]
            
            # SQLite will output to results/output.log as specified in the SQL file
            # We just need to execute and capture stderr for errors
            with open(sql_path, 'r') as sql_input:
                process = subprocess.Popen(
                    command,
                    stdin=sql_input,
                    stdout=subprocess.DEVNULL,  # SQLite uses .output in SQL file
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
                _, stderr = process.communicate()
                
                # Stop CPU monitoring
                try:
                    result = monitor.stop()
                    if result:
                        cpu_result = result.peak_cpu_percent
                except Exception:
                    pass
                
                if process.returncode != 0:
                    raise RuntimeError(f"SQLite execution failed: {stderr}")
                
                return cpu_result
                
        finally:
            # Restore original directory
            os.chdir(original_dir)
