from benchmark.file_utils.file_utils import clean_path
from benchmark.service.monitor.process_monitor import monitor_subprocess
from benchmark.service.monitor.process_monitor_result import ProcessMonitorResult
from benchmark.service.proflie_parser.query_metric import QueryMetrics
from benchmark.service.proflie_parser.sqlite_log_parser import SqliteLogParser
from benchmark.service.runner.sqlite_runner import SQLiteRunner
from benchmark.service.task_executor.task_execute_result import TaskExecuteResult


class TaskExecutor:
    def __init__(self, runner, log_parser, repeat=1):
        self.runner = runner
        self.log_parser = log_parser
        self.repeat = repeat

    def execute(self) -> TaskExecuteResult:
        clean_path(self.runner.cwd / "results")
        results = []
        for i in range(self.repeat):
            print(f"--- Execution round {i + 1} ---")
            process = self.runner.run_subprocess()
            monitor_result = monitor_subprocess(process)
            query_metric = self.log_parser.parse_log()
            task_execute_result = self.combine_results(monitor_result, query_metric)
            print(f"--- Execution round {i + 1} results: {task_execute_result} ---")
            results.append(task_execute_result)
        
        # Calculate averages across all runs
        avg_result = TaskExecuteResult(
            cpu_peek_percent=sum(r.cpu_peek_percent for r in results) / len(results),
            cpu_avg_percent=sum(r.cpu_avg_percent for r in results) / len(results),
            cpu_samples_count= int(sum(r.cpu_samples_count for r in results) / len(results)),
            cpu_sampling_interval=sum(r.cpu_sampling_interval for r in results) / len(results),
            peak_memory_bytes=sum(r.peak_memory_bytes for r in results) / len(results),
            execution_time=sum(r.execution_time for r in results) / len(results),
            output_rows=results[0].output_rows if results else 0,  # output_rows should be same across runs
        )
        print(f"--- Execution final averaged results: {avg_result} ---")
        return avg_result

    def combine_results(self, monitor_result : ProcessMonitorResult, query_metric : QueryMetrics) -> TaskExecuteResult:
        return TaskExecuteResult(
            cpu_peek_percent=monitor_result.peak_cpu_percent,
            cpu_avg_percent=monitor_result.avg_cpu_percent,
            cpu_samples_count=monitor_result.samples_count,
            cpu_sampling_interval=monitor_result.sampling_interval,
            peak_memory_bytes = query_metric.memory.max_memory_used,
            execution_time = query_metric.timing.run_time,
            output_rows = query_metric.output_rows,
        )

if __name__ == "__main__":
    sql_file = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/queries/Q1/Q1_sqlite.sql"
    db_file = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/db_vs14/vs14_data.sqlite"
    sqlite_cmd = "/Users/xiejiangzhao/sqlite3/bin/sqlite3"
    cwd = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/test"
    runner = SQLiteRunner(sql_file=sql_file, db_file=db_file, cmd=sqlite_cmd, cwd=cwd)
    sqlite_parser = SqliteLogParser(log_path=runner.results_dir)
    task_executor = TaskExecutor(runner=runner, log_parser=sqlite_parser, repeat=3)
    task_executor.execute()
