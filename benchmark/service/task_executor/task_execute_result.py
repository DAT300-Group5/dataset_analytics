import dataclasses


@dataclasses.dataclass
class StatSummary:
    """Statistical summary of a list of numeric values"""
    raw_data: list[float]
    min: float
    max: float
    p50: float
    p95: float
    p99: float
    avg: float

    def to_summary_dict(self):
        """Convert to dictionary for JSON serialization"""
        data = dataclasses.asdict(self)
        data.pop("raw_data")
        return data

    def to_raw_data_dict(self):
        """Convert only raw data to dictionary for JSON serialization"""
        return {"raw_data": self.raw_data}

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

    def to_summary_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            "cpu_peak_percent": self.cpu_peak_percent.to_summary_dict(),
            "cpu_avg_percent": self.cpu_avg_percent.to_summary_dict(),
            "cpu_samples_count": self.cpu_samples_count,
            "cpu_sampling_interval": self.cpu_sampling_interval,
            "peak_memory_bytes": self.peak_memory_bytes.to_summary_dict(),
            "execution_time": self.execution_time.to_summary_dict(),
            "monitor_record_execution_time": self.monitor_record_execution_time.to_summary_dict(),
            "output_rows": self.output_rows
        }

    def to_raw_data_dict(self):
        """Convert only raw data to dictionary for JSON serialization"""
        return {
            "cpu_peak_percent": self.cpu_peak_percent.to_raw_data_dict(),
            "cpu_avg_percent": self.cpu_avg_percent.to_raw_data_dict(),
            "cpu_samples_count": self.cpu_samples_count,
            "cpu_sampling_interval": self.cpu_sampling_interval,
            "peak_memory_bytes": self.peak_memory_bytes.to_raw_data_dict(),
            "execution_time": self.execution_time.to_raw_data_dict(),
            "monitor_record_execution_time": self.monitor_record_execution_time.to_raw_data_dict(),
            "output_rows": self.output_rows
        }

@dataclasses.dataclass
class SingleTaskExecuteResult:
    cpu_peak_percent: float
    cpu_avg_percent: float
    cpu_samples_count: int
    cpu_sampling_interval: float
    peak_memory_bytes: int
    execution_time: float
    monitor_record_execution_time: float
    output_rows: int

    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return dataclasses.asdict(self)