import dataclasses


@dataclasses.dataclass
class TaskExecuteResult:
    cpu_peek_percent: float
    cpu_avg_percent: float
    cpu_samples_count: int
    cpu_sampling_interval: float
    peak_memory_bytes: float
    execution_time: float
    output_rows: int