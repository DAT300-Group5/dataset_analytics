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

from benchmark.monitor.process_monitor_result import ProcessMonitorResult
from benchmark.monitor.process_snapshot import ProcessSnapshot


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
            print(f"⚠ Warning: Process {self.pid} not found")
            return

        self.start_time = time.time()
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
        self.end_time = time.time()

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

                # Get memory info (only RSS for peak tracking)
                mem_info = self.process.memory_info()
                rss_mb = mem_info.rss / (1024 * 1024)

                # Record simplified snapshot
                snapshot = ProcessSnapshot(
                    timestamp=time.time(),
                    cpu_percent=cpu_percent,
                    rss_mb=rss_mb
                )
                self.snapshots.append(snapshot)

                # Sleep until next sample
                time.sleep(self.interval)

            except psutil.NoSuchProcess:
                # Process ended
                break
            except Exception as e:
                print(f"⚠ Process monitor error: {e}")
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
        rss_values = [s.rss_mb for s in self.snapshots]

        # Calculate process duration
        duration = 0.0
        if self.start_time and self.end_time:
            duration = self.end_time - self.start_time

        result = ProcessMonitorResult(
            # CPU statistics
            peak_cpu_percent=max(cpu_values),
            avg_cpu_percent=sum(cpu_values) / len(cpu_values),
            min_cpu_percent=min(cpu_values),
            samples_count=len(self.snapshots),
            sampling_interval=self.interval,

            # Memory statistics (only peak)
            peak_memory_mb=max(rss_values),

            # Process timing
            process_duration_seconds=duration,

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
