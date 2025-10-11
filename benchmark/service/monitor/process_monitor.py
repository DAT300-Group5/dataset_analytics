"""
Process Monitor Module

This module provides process resource usage monitoring for subprocess execution.
Shared by both SQLite and DuckDB benchmarks.
"""
import subprocess
import threading
import time
from typing import Optional, List

import psutil

from benchmark.service.monitor.process_monitor_result import ProcessMonitorResult
from benchmark.service.monitor.process_snapshot import ProcessSnapshot
from benchmark.service.runner.sqlite_runner import SQLiteRunner
from benchmark.util.log_config import setup_logger

logger = setup_logger(__name__)


class ProcessMonitor:
    """Monitor resource usage of a process"""

    def __init__(self, pid: int, interval: float = 0.1):
        """
        Initialize process monitor.

        Args:
            pid: Process ID to monitor
            interval: Sampling interval in seconds (default: 0.1s = 100ms)
        """
        self.pid = pid
        self.interval = interval
        self.snapshots: List[ProcessSnapshot] = []
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.process: Optional[psutil.Process] = None
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

    def start(self):
        """Start monitoring in a background thread"""
        if self.running:
            return

        try:
            self.process = psutil.Process(self.pid)
            # Initialize CPU percent (first call returns 0.0)
            self.process.cpu_percent(interval=None)
        except psutil.NoSuchProcess:
            logger.warning(f"Process {self.pid} not found")
            return

        self.start_time = time.perf_counter()
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()

    def stop(self) -> Optional[ProcessMonitorResult]:
        """
        Stop monitoring and return results.

        Returns:
            ProcessMonitorResult or None if no samples collected
        """
        self.running = False
        self.end_time = time.perf_counter()

        if self.thread:
            self.thread.join(timeout=2.0)

        return self.get_results()

    def _monitor_loop(self):
        """Main monitoring loop (runs in background thread)"""
        while self.running:
            try:
                if self.process is None or not self.process.is_running():
                    break

                # Get CPU usage
                cpu_percent = self.process.cpu_percent(interval=None)

                # Record simplified snapshot
                snapshot = ProcessSnapshot(
                    timestamp=time.time(),
                    cpu_percent=cpu_percent
                )
                self.snapshots.append(snapshot)

                # Sleep until next sample
                time.sleep(self.interval)

            except psutil.NoSuchProcess:
                # Process ended
                break
            except Exception as e:
                logger.warning(f"Monitor error: {e}")
                break

    def get_results(self) -> Optional[ProcessMonitorResult]:
        """
        Get monitoring results with simplified memory statistics.

        Returns:
            ProcessMonitorResult or None if no samples
        """
        if not self.snapshots:
            return None

        # Extract values for calculations
        cpu_values = [s.cpu_percent for s in self.snapshots]

        result = ProcessMonitorResult(
            # CPU statistics
            peak_cpu_percent=max(cpu_values),
            avg_cpu_percent=sum(cpu_values) / len(cpu_values),
            samples_count=len(self.snapshots),
            sampling_interval=self.interval,
            execution_time=self.end_time - self.start_time,
            # All snapshots
            snapshots=self.snapshots
        )

        return result


def monitor_subprocess(process: 'subprocess.Popen', interval: float = 0.1) -> Optional[ProcessMonitorResult]:
    """
    Monitor a subprocess and return process resource usage statistics.

    Args:
        process: subprocess.Popen instance
        interval: Sampling interval in seconds

    Returns:
        ProcessMonitorResult or None if monitoring failed
    """
    monitor = ProcessMonitor(process.pid, interval=interval)
    monitor.start()

    # Wait for process to complete
    process.wait()

    # Stop monitoring and get results
    return monitor.stop()

if __name__ == "__main__":
    sql_file = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/queries/Q1/Q1_sqlite.sql"
    db_file = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/db_vs14/vs14_data.sqlite"
    sqlite_cmd = "/Users/xiejiangzhao/sqlite3/bin/sqlite3"
    cwd = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/test"
    runner = SQLiteRunner(sql_file=sql_file, db_file=db_file, cmd=sqlite_cmd, cwd=cwd)
    process = runner.run_subprocess()
    monitor = monitor_subprocess(process, interval=0.002)
    stdout, stderr = process.communicate()
    if process.returncode == 0:
        logger.info("Execution succeeded")
        if stdout:
            logger.debug(stdout)
    else:
        logger.error("Execution failed")
        if stderr:
            logger.error(stderr)
    if monitor:
        logger.info(f"Peak CPU: {monitor.peak_cpu_percent:.1f}%")
        logger.info(f"Avg CPU: {monitor.avg_cpu_percent:.1f}%")
        logger.info(f"Samples: {monitor.samples_count}")
        logger.info(f"Interval: {monitor.sampling_interval}s")
