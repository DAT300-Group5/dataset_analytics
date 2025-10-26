from service.monitor.process_monitor import monitor_subprocess
from service.task_executor.task_execute_result import TaskExecuteResult
from service.runner.runner import Runner
from service.profile_parser.log_parser import LogParser
from util.cal_utils import calculate_stat_summary, combine_results
from util.file_utils import clean_path
from util.log_config import setup_logger

DEFAULT_PIVOT_INTERVAL = 10  # seconds

logger = setup_logger(__name__)

class TaskExecutor:
    def __init__(self, runner: Runner, log_parser: LogParser, sample_count: int = 20, pivot_repeat: int = 3, std_repeat: int = 1):
        self.runner = runner
        self.log_parser = log_parser
        self.std_repeat = std_repeat
        self.pivot_repeat = pivot_repeat
        self.sample_count = sample_count

    def calculate_interval(self) -> float:
        logger.info(f"Stage 1/2: Calculating sampling interval (pivot runs: {self.pivot_repeat})")
        pivot_result = self._execute(self.pivot_repeat, interval=DEFAULT_PIVOT_INTERVAL, is_pivot=True)
        avg_execution_time = pivot_result.execution_time.avg
        interval = avg_execution_time / self.sample_count
        logger.info(f"✓ Stage 1/2 completed: interval={interval:.3f}s (avg time={avg_execution_time:.3f}s)")
        return interval

    def std_execute(self) -> TaskExecuteResult:
        interval = self.calculate_interval()
        logger.info(f"Stage 2/2: Running benchmark ({self.std_repeat} iterations, interval={interval:.3f}s)")
        result = self._execute(self.std_repeat, interval=interval, is_pivot=False)
        logger.info(f"✓ Stage 2/2 completed: "
                   f"Time(avg)={result.execution_time.avg:.3f}s, "
                   f"CPU(avg)={result.cpu_avg_percent.avg:.1f}%, "
                   f"Memory(peak)={result.peak_memory_bytes.max / 1024 / 1024:.1f}MB")
        return result

    def _execute(self, repeat: int, interval: float, is_pivot: bool = False) -> TaskExecuteResult:
        clean_path(self.runner.results_dir)
        results = []
        for i in range(repeat):
            logger.info(f"  Run {i + 1}/{repeat}: Executing query...")
            process = self.runner.run_subprocess()
            monitor_result = monitor_subprocess(process, interval=interval)
            if monitor_result is None:
                logger.error("monitor_subprocess returned None for run %d/%d; aborting.", i + 1, repeat)
                raise RuntimeError("monitor_subprocess returned None")
            query_metric = self.log_parser.parse_log()
            task_execute_result = combine_results(monitor_result, query_metric)
            logger.info(f"  Run {i + 1}/{repeat}: Time={task_execute_result.execution_time:.2f}s, "
                        f"CPU={task_execute_result.cpu_avg_percent:.1f}%, "
                        f"Memory={task_execute_result.peak_memory_bytes / 1024 / 1024:.1f}MB, "
                        f"Rows={task_execute_result.output_rows}")
            results.append(task_execute_result)
        
        # Calculate statistical summaries across all runs
        logger.info(f"  Aggregating {repeat} run(s)...")
        summary_result = TaskExecuteResult(
            cpu_peak_percent=calculate_stat_summary([r.cpu_peak_percent for r in results]),
            cpu_avg_percent=calculate_stat_summary([r.cpu_avg_percent for r in results]),
            cpu_samples_count=int(sum(r.cpu_samples_count for r in results) / len(results)),
            cpu_sampling_interval=sum(r.cpu_sampling_interval for r in results) / len(results),
            peak_memory_bytes=calculate_stat_summary([r.peak_memory_bytes for r in results]),
            execution_time=calculate_stat_summary([r.execution_time for r in results]),
            monitor_record_execution_time=calculate_stat_summary([r.execution_time for r in results]),
            output_rows=results[0].output_rows if results else 0,  # output_rows should be same across runs
        )
        logger.info(f"  → Avg={summary_result.execution_time.avg:.3f}s, "
                   f"P50={summary_result.execution_time.p50:.3f}s, "
                   f"P95={summary_result.execution_time.p95:.3f}s")
        return summary_result

if __name__ == "__main__":
    
    # python3 -m service.task_executor.task_executor

    from util.file_utils import project_root
    from service.profile_parser.sqlite_log_parser import SqliteLogParser
    from service.runner.sqlite_runner import SQLiteRunner
    
    root = project_root()

    sql_file = root / "benchmark/queries/Q1/Q1_sqlite.sql"
    db_file = root / "benchmark/db_vs14/vs14_data.sqlite"
    sqlite_cmd = "sqlite3"
    cwd = root / "benchmark/test"

    runner = SQLiteRunner(sql_file=sql_file, db_file=db_file, cmd=sqlite_cmd, cwd=cwd)
    sqlite_parser = SqliteLogParser(log_path=runner.results_dir)
    task_executor = TaskExecutor(runner=runner, log_parser=sqlite_parser, sample_count=10, std_repeat=3)
    task_executor.std_execute()
