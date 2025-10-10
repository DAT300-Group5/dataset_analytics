from dataclasses import dataclass
from typing import Optional


@dataclass
class TimingInfo:
    """Data class to store timing information from SQLite"""
    run_time: Optional[float] = None  # seconds
    user_time: Optional[float] = None  # seconds
    system_time: Optional[float] = None  # seconds


@dataclass
class MemoryInfo:
    """Data class to store memory statistics from SQLite"""
    memory_used: Optional[int] = None  # bytes
    heap_usage: Optional[int] = None  # bytes
    page_cache_hits: Optional[int] = None
    page_cache_misses: Optional[int] = None
    page_cache_size: Optional[int] = None


@dataclass
class QueryMetrics:
    """Complete metrics for a query execution"""
    query_number: Optional[int] = None
    timing: Optional[TimingInfo] = None
    memory: Optional[MemoryInfo] = None
    output_rows: Optional[int] = None  # Number of output rows
