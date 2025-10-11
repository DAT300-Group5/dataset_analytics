from dataclasses import dataclass
from typing import List, Dict

from benchmark.service.monitor.process_snapshot import ProcessSnapshot


@dataclass
class ProcessMonitorResult:
    """Process resource monitoring results"""
    # CPU statistics
    peak_cpu_percent: float
    avg_cpu_percent: float
    samples_count: int
    sampling_interval: float
    execution_time: float

    # All snapshots for detailed analysis
    snapshots: List[ProcessSnapshot]

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            # CPU stats
            'peak_cpu_percent': self.peak_cpu_percent,
            'avg_cpu_percent': self.avg_cpu_percent,
            'samples_count': self.samples_count,
            'sampling_interval': self.sampling_interval,

            # Detailed snapshots
            'snapshots': [
                {
                    'timestamp': s.timestamp,
                    'cpu_percent': s.cpu_percent,
                }
                for s in self.snapshots
            ]
        }
