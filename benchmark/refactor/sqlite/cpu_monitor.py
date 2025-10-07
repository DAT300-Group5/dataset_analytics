"""
CPU Monitor Module

This module provides CPU usage monitoring for subprocess execution.
"""
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Optional, Dict, List

import psutil


@dataclass
class CPUSnapshot:
    """Single CPU usage snapshot"""
    timestamp: float
    cpu_percent: float
    num_threads: int
    memory_mb: float


@dataclass
class CPUMonitorResult:
    """CPU monitoring results"""
    peak_cpu_percent: float
    avg_cpu_percent: float
    min_cpu_percent: float
    samples_count: int
    sampling_interval: float
    peak_memory_mb: float
    avg_memory_mb: float
    snapshots: List[CPUSnapshot]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'peak_cpu_percent': self.peak_cpu_percent,
            'avg_cpu_percent': self.avg_cpu_percent,
            'min_cpu_percent': self.min_cpu_percent,
            'samples_count': self.samples_count,
            'sampling_interval': self.sampling_interval,
            'peak_memory_mb': self.peak_memory_mb,
            'avg_memory_mb': self.avg_memory_mb,
            'snapshots': [
                {
                    'timestamp': s.timestamp,
                    'cpu_percent': s.cpu_percent,
                    'num_threads': s.num_threads,
                    'memory_mb': s.memory_mb
                }
                for s in self.snapshots
            ]
        }


class CPUMonitor:
    """Monitor CPU usage of a process"""
    
    def __init__(self, pid: int, interval: float = 0.1):
        """
        Initialize CPU monitor.
        
        Args:
            pid: Process ID to monitor
            interval: Sampling interval in seconds (default: 0.1s = 100ms)
        """
        self.pid = pid
        self.interval = interval
        self.snapshots: List[CPUSnapshot] = []
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.process: Optional[psutil.Process] = None
        
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
        
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
    
    def stop(self) -> Optional[CPUMonitorResult]:
        """
        Stop monitoring and return results.
        
        Returns:
            CPUMonitorResult or None if no samples collected
        """
        self.running = False
        
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
                
                # Get memory info
                mem_info = self.process.memory_info()
                memory_mb = mem_info.rss / (1024 * 1024)  # Convert to MB
                
                # Get thread count
                num_threads = self.process.num_threads()
                
                # Record snapshot
                snapshot = CPUSnapshot(
                    timestamp=time.time(),
                    cpu_percent=cpu_percent,
                    num_threads=num_threads,
                    memory_mb=memory_mb
                )
                self.snapshots.append(snapshot)
                
                # Sleep until next sample
                time.sleep(self.interval)
                
            except psutil.NoSuchProcess:
                # Process ended
                break
            except Exception as e:
                print(f"⚠ CPU monitor error: {e}")
                break
    
    def get_results(self) -> Optional[CPUMonitorResult]:
        """
        Get monitoring results.
        
        Returns:
            CPUMonitorResult or None if no samples
        """
        if not self.snapshots:
            return None
        
        cpu_values = [s.cpu_percent for s in self.snapshots]
        memory_values = [s.memory_mb for s in self.snapshots]
        
        result = CPUMonitorResult(
            peak_cpu_percent=max(cpu_values),
            avg_cpu_percent=sum(cpu_values) / len(cpu_values),
            min_cpu_percent=min(cpu_values),
            samples_count=len(self.snapshots),
            sampling_interval=self.interval,
            peak_memory_mb=max(memory_values),
            avg_memory_mb=sum(memory_values) / len(memory_values),
            snapshots=self.snapshots
        )
        
        return result


def monitor_subprocess(process: 'subprocess.Popen', interval: float = 0.1) -> Optional[CPUMonitorResult]:
    """
    Monitor a subprocess and return CPU usage statistics.
    
    Args:
        process: subprocess.Popen instance
        interval: Sampling interval in seconds
        
    Returns:
        CPUMonitorResult or None if monitoring failed
    """
    monitor = CPUMonitor(process.pid, interval=interval)
    monitor.start()
    
    # Wait for process to complete
    process.wait()
    
    # Stop monitoring and get results
    return monitor.stop()
