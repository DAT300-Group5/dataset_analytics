"""
Database Benchmark Interface

This module defines a common interface for database benchmarking across different engines.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List, Dict
from pathlib import Path


@dataclass
class QueryResult:
    """Result metrics for a single query"""
    query_number: int
    query_sql: str
    wall_time: float  # seconds
    peak_memory_bytes: int  # bytes
    output_rows: int
    throughput: float  # rows per second
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'query_number': self.query_number,
            'query_sql': self.query_sql,
            'wall_time': self.wall_time,
            'peak_memory_bytes': self.peak_memory_bytes,
            'peak_memory_mb': round(self.peak_memory_bytes / (1024 * 1024), 2),
            'output_rows': self.output_rows,
            'throughput_rows_per_sec': round(self.throughput, 2)
        }


@dataclass
class BenchmarkResult:
    """Complete benchmark results"""
    engine_name: str
    db_file: str
    sql_file: str
    queries: List[QueryResult]
    total_wall_time: float
    peak_memory_bytes: int
    total_output_rows: int
    overall_throughput: float
    peak_cpu_percent: Optional[float] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'engine': self.engine_name,
            'database': self.db_file,
            'sql_file': self.sql_file,
            'summary': {
                'total_queries': len(self.queries),
                'total_wall_time': round(self.total_wall_time, 6),
                'peak_memory_bytes': self.peak_memory_bytes,
                'peak_memory_mb': round(self.peak_memory_bytes / (1024 * 1024), 2),
                'total_output_rows': self.total_output_rows,
                'overall_throughput_rows_per_sec': round(self.overall_throughput, 2),
                'peak_cpu_percent': round(self.peak_cpu_percent, 2) if self.peak_cpu_percent else None
            },
            'queries': [q.to_dict() for q in self.queries]
        }


class DatabaseBenchmark(ABC):
    """Abstract base class for database benchmarking (CPU monitoring always enabled)"""
    
    def __init__(self, 
                 db_file: str,
                 sql_file: str):
        """
        Initialize database benchmark.
        
        Args:
            db_file: Path to database file (will be created if doesn't exist)
            sql_file: Path to SQL script to execute
        """
        self.db_file = Path(db_file)
        self.sql_file = Path(sql_file)
        
        # Validate SQL file exists
        if not self.sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
    
    @abstractmethod
    def run(self) -> BenchmarkResult:
        """
        Execute the benchmark and return results.
        
        Returns:
            BenchmarkResult with all metrics
        """
        pass
    
    @abstractmethod
    def get_engine_name(self) -> str:
        """Get the database engine name"""
        pass
    
    def cleanup(self):
        """Clean up temporary files and databases"""
        if self.db_file.exists():
            self.db_file.unlink()


def run_benchmark(engine: str,
                 db_file: str,
                 sql_file: str,
                 **kwargs) -> BenchmarkResult:
    """
    Factory function to run a benchmark with the specified engine.
    CPU monitoring is always enabled.
    
    Args:
        engine: Database engine name ('sqlite' or 'duckdb')
        db_file: Path to database file
        sql_file: Path to SQL script
        **kwargs: Additional engine-specific arguments (e.g., sqlite_cmd, duckdb_cmd)
        
    Returns:
        BenchmarkResult with 5 core metrics (Wall Time, Peak Memory, Output Rows, Throughput, Peak CPU)
        
    Raises:
        ValueError: If engine is not supported
    """
    if engine.lower() == 'sqlite':
        from sqlite_benchmark import SQLiteBenchmark
        sqlite_cmd = kwargs.get('sqlite_cmd', 'sqlite3')
        benchmark = SQLiteBenchmark(db_file, sql_file, sqlite_cmd)
    elif engine.lower() == 'duckdb':
        from duckdb_benchmark import DuckDBBenchmark
        duckdb_cmd = kwargs.get('duckdb_cmd', '../duckdb')
        benchmark = DuckDBBenchmark(db_file, sql_file, duckdb_cmd)
    else:
        raise ValueError(f"Unsupported engine: {engine}. Use 'sqlite' or 'duckdb'")
    
    return benchmark.run()
