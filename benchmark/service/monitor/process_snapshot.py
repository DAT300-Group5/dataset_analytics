from dataclasses import dataclass


@dataclass
class ProcessSnapshot:
    """Single process resource usage snapshot"""
    timestamp: float
    cpu_percent: float