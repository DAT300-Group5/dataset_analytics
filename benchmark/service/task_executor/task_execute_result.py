import dataclasses



@dataclasses.dataclass
class StatSummary:
    """Statistical summary of a list of numeric values"""
    min: float
    max: float
    p50: float
    p95: float
    p99: float
    avg: float

    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return dataclasses.asdict(self)

@dataclasses.dataclass
class TaskExecuteResult:
    cpu_peak_percent: StatSummary
    cpu_avg_percent: StatSummary
    cpu_samples_count: int
    cpu_sampling_interval: float
    peak_memory_bytes: StatSummary
    execution_time: StatSummary
    monitor_record_execution_time: StatSummary
    output_rows: int

    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return dataclasses.asdict(self)

@dataclasses.dataclass
class SingleTaskExecuteResult:
    cpu_peak_percent: float
    cpu_avg_percent: float
    cpu_samples_count: int
    cpu_sampling_interval: float
    peak_memory_bytes: float
    execution_time: float
    monitor_record_execution_time: float
    output_rows: int

    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return dataclasses.asdict(self)