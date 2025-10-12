"""Benchmark result data models."""

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
import json


@dataclass
class BenchmarkRun:
    """
    Represents a single benchmark run result.
    
    Contains all metrics collected during one execution of a query.
    """
    # Basic execution info
    retval: int
    wall_time_seconds: float
    ttfr_seconds: Optional[float]
    rows_returned: int
    statements_executed: int
    select_statements: int
    mode: str  # "child" or "inproc"
    
    # Memory metrics
    peak_rss_bytes_sampled: int
    peak_rss_bytes_true: int
    python_heap_peak_bytes: int
    
    # CPU metrics
    cpu_avg_percent: float
    samples: int
    
    # Child-specific metrics (only present in child mode)
    child_wall_time_seconds: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BenchmarkRun':
        """Create BenchmarkRun from dictionary."""
        return cls(**data)


@dataclass 
class BenchmarkSummary:
    """
    Represents aggregated benchmark summary statistics.
    
    Contains statistical summaries (mean, percentiles) for all metrics
    across multiple benchmark runs.
    """
    # Test configuration
    engine: str
    mode: str  # "child" or "inproc"
    db_path: str
    query_file: str
    repeat: int
    warmups: int
    threads: int
    
    # Wall time statistics
    mean_wall_time_seconds: Optional[float]
    p50_wall_time_seconds: Optional[float]
    p95_wall_time_seconds: Optional[float]
    p99_wall_time_seconds: Optional[float]
    
    # Time to first row statistics
    mean_ttfr_seconds: Optional[float]
    p50_ttfr_seconds: Optional[float]
    p95_ttfr_seconds: Optional[float]
    p99_ttfr_seconds: Optional[float]
    
    # Resource usage averages
    mean_peak_rss_bytes_true: Optional[float]
    mean_cpu_avg_percent: Optional[float]
    mean_rows_returned: Optional[float]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BenchmarkSummary':
        """Create BenchmarkSummary from dictionary."""
        return cls(**data)
    
    def format_wall_time_stats(self) -> str:
        """Format wall time statistics for display."""
        def fmt(v, unit="s"):
            return "None" if v is None else f"{v:.3f}{unit}"
        
        return "wall: mean={}  p50={}  p95={}  p99={}".format(
            fmt(self.mean_wall_time_seconds),
            fmt(self.p50_wall_time_seconds),
            fmt(self.p95_wall_time_seconds),
            fmt(self.p99_wall_time_seconds),
        )
    
    def format_ttfr_stats(self) -> str:
        """Format TTFR statistics for display."""
        def fmt(v, unit="s"):
            return "None" if v is None else f"{v:.3f}{unit}"
        
        return "ttfr: mean={}  p50={}  p95={}  p99={}".format(
            fmt(self.mean_ttfr_seconds),
            fmt(self.p50_ttfr_seconds),
            fmt(self.p95_ttfr_seconds),
            fmt(self.p99_ttfr_seconds),
        )
    
    def format_resource_stats(self) -> List[str]:
        """Format resource usage statistics for display."""
        stats = []
        
        if self.mean_peak_rss_bytes_true is not None:
            stats.append(f"peak_rss_true: mean={self.mean_peak_rss_bytes_true / (1024**2):.1f} MB")
        
        if self.mean_cpu_avg_percent is not None:
            stats.append(f"cpu_avg_percent: mean={self.mean_cpu_avg_percent:.1f}%")
        
        if self.mean_rows_returned is not None:
            stats.append(f"rows_returned: mean={self.mean_rows_returned:.1f}")
        
        return stats


@dataclass
class BenchmarkResult:
    """
    Complete benchmark result containing both runs and summary.
    
    This is the top-level structure that gets serialized to JSON output files.
    """
    runs: List[BenchmarkRun]
    summary: BenchmarkSummary
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "runs": [run.to_dict() for run in self.runs],
            "summary": self.summary.to_dict()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BenchmarkResult':
        """Create BenchmarkResult from dictionary."""
        runs = [BenchmarkRun.from_dict(run_data) for run_data in data["runs"]]
        summary = BenchmarkSummary.from_dict(data["summary"])
        return cls(runs=runs, summary=summary)
    
    def save_to_file(self, file_path: str) -> None:
        """Save benchmark result to JSON file."""
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
    
    @classmethod
    def load_from_file(cls, file_path: str) -> 'BenchmarkResult':
        """Load benchmark result from JSON file."""
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dict(data)
    
    def print_summary(self) -> None:
        """Print formatted summary to console."""
        print("\n=== Summary ===")
        print(self.summary.format_wall_time_stats())
        print(self.summary.format_ttfr_stats())
        
        for stat_line in self.summary.format_resource_stats():
            print(stat_line)