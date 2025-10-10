from dataclasses import dataclass


@dataclass
class ProcessSnapshot:
    """Single process resource usage snapshot"""
    timestamp: float
    cpu_percent: float
    rss_mb: float  # Resident Set Size (physical memory)