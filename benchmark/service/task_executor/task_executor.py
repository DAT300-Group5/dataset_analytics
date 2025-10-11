from benchmark.service.monitor.process_monitor import monitor_subprocess
from benchmark.service.proflie_parser.sqlite_log_parser import SqliteLogParser
from benchmark.service.runner.sqlite_runner import SQLiteRunner
from benchmark.service.task_executor.task_execute_result import TaskExecuteResult
from benchmark.util.cal_utils import calculate_stat_summary, combine_results
from benchmark.util.file_utils import clean_path

DEFAULT_PIVOT_INTERVAL = 10  # seconds

class TaskExecutor:
    def __init__(self, runner, log_parser, sample_count: int = 20, pivot_repeat : int = 3, std_repeat: int = 1):
        self.runner = runner
        self.log_parser = log_parser
        self.std_repeat = std_repeat
        self.pivot_repeat = pivot_repeat
        self.sample_count = sample_count

    def calculate_interval(self) -> float:
        pivot_result = self._execute(self.pivot_repeat, interval=DEFAULT_PIVOT_INTERVAL)
        avg_execution_time = pivot_result.execution_time.avg
        interval = avg_execution_time / self.sample_count
        print(f"Pivot average execution time: {avg_execution_time:.3f} seconds, chosen interval: {interval:.3f} seconds")
        return interval

    def std_execute(self) -> TaskExecuteResult:
        interval = self.calculate_interval()
        print(f"Calculated monitoring interval: {interval:.3f} seconds")
        return self._execute(self.std_repeat, interval=interval)

    def _execute(self, repeat, interval) -> TaskExecuteResult:
        clean_path(self.runner.cwd / "results")
        results = []
        for i in range(repeat):
            print(f"--- Execution round {i + 1} ---")
            process = self.runner.run_subprocess()
            monitor_result = monitor_subprocess(process, interval=interval)
            query_metric = self.log_parser.parse_log()
            task_execute_result = combine_results(monitor_result, query_metric)
            print(f"--- Execution round {i + 1} results: {task_execute_result} ---")
            results.append(task_execute_result)
        
        # Calculate statistical summaries across all runs
        summary_result = TaskExecuteResult(
            cpu_peek_percent=calculate_stat_summary([r.cpu_peek_percent for r in results]),
            cpu_avg_percent=calculate_stat_summary([r.cpu_avg_percent for r in results]),
            cpu_samples_count=int(sum(r.cpu_samples_count for r in results) / len(results)),
            cpu_sampling_interval=sum(r.cpu_sampling_interval for r in results) / len(results),
            peak_memory_bytes=calculate_stat_summary([r.peak_memory_bytes for r in results]),
            execution_time=calculate_stat_summary([r.execution_time for r in results]),
            monitor_record_execution_time=calculate_stat_summary([r.execution_time for r in results]),
            output_rows=results[0].output_rows if results else 0,  # output_rows should be same across runs
        )
        print(f"--- Execution final statistical summary: {summary_result} ---")
        return summary_result

if __name__ == "__main__":
    sql_file = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/queries/Q1/Q1_sqlite.sql"
    db_file = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/db_vs14/vs14_data.sqlite"
    sqlite_cmd = "/Users/xiejiangzhao/sqlite3/bin/sqlite3"
    cwd = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/test"
    runner = SQLiteRunner(sql_file=sql_file, db_file=db_file, cmd=sqlite_cmd, cwd=cwd)
    sqlite_parser = SqliteLogParser(log_path=runner.results_dir)
    task_executor = TaskExecutor(runner=runner, log_parser=sqlite_parser, sample_count=10, std_repeat=3)
    task_executor.std_execute()
