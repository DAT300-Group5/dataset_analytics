from service.monitor.process_monitor_result import ProcessMonitorResult
from service.profile_parser.query_metric import QueryMetrics
from service.task_executor.task_execute_result import StatSummary, SingleTaskExecuteResult


def calculate_stat_summary(values: list[float]) -> StatSummary:
    """Calculate statistical summary from a list of numeric values"""
    if not values:
        return StatSummary(min=0, max=0, p50=0, p95=0, p99=0, avg=0)

    sorted_values = sorted(values)
    n = len(sorted_values)

    return StatSummary(
        min=sorted_values[0],
        max=sorted_values[-1],
        p50=sorted_values[int(n * 0.50)],
        p95=sorted_values[int(n * 0.95)] if n > 1 else sorted_values[0],
        p99=sorted_values[int(n * 0.99)] if n > 1 else sorted_values[0],
        avg=sum(sorted_values) / n
    )

def combine_results(monitor_result : ProcessMonitorResult, query_metric : QueryMetrics) -> SingleTaskExecuteResult:
    return SingleTaskExecuteResult(
        cpu_peak_percent=monitor_result.peak_cpu_percent,
        cpu_avg_percent=monitor_result.avg_cpu_percent,
        cpu_samples_count=monitor_result.samples_count,
        cpu_sampling_interval=monitor_result.sampling_interval,
        peak_memory_bytes=query_metric.memory.max_memory_used,
        execution_time=query_metric.timing.run_time,
        output_rows=query_metric.output_rows,
        monitor_record_execution_time=monitor_result.execution_time
    )