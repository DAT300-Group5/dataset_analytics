from pathlib import Path
import re

from benchmark.service.proflie_parser.query_metric import QueryMetrics, TimingInfo, MemoryInfo


class SqliteLogParser:
    def __init__(self, log_path):
        self.log_path = Path(log_path)

    def parse_log(self) -> QueryMetrics:
        """Parse SQLite log files and extract metrics."""
        stdout_file = self.log_path / "stdout.log"
        
        if not stdout_file.exists():
            raise FileNotFoundError(f"Log file {stdout_file} does not exist.")
        
        # Parse stdout file - it contains both data rows and statistics
        output_rows, timing_info, memory_info = self._parse_stdout(stdout_file)
        
        return QueryMetrics(
            timing=timing_info,
            memory=memory_info,
            output_rows=output_rows
        )
    
    def _parse_stdout(self, stdout_file: Path) -> tuple[int, TimingInfo, MemoryInfo]:
        """Parse stdout.log which contains both data rows and statistics."""
        timing_info = TimingInfo()
        memory_info = MemoryInfo()
        output_rows = 0
        
        try:
            with open(stdout_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Find where statistics start (after the data rows)
            stats_start_idx = len(lines)
            for i, line in enumerate(lines):
                # Statistics section starts with lines like "Memory Used:" or "Run Time:"
                if re.match(r'^(Memory Used:|Run Time:|Number of|Largest|Lookaside|Pager|Page cache|Schema|Statement|Fullscan|Sort|Autoindex|Bloom|Virtual|Reprepare)', line):
                    stats_start_idx = i
                    break
            
            # Count data rows (lines before statistics section)
            output_rows = sum(1 for line in lines[:stats_start_idx] if line.strip())
            
            # Parse statistics section
            stats_content = ''.join(lines[stats_start_idx:])
            
            # Parse timing information
            # Format: "Run Time: real 17.338 user 14.308602 sys 2.313507"
            timing_match = re.search(r'Run Time: real\s+([\d.]+)\s+user\s+([\d.]+)\s+sys\s+([\d.]+)', stats_content)
            if timing_match:
                timing_info.run_time = float(timing_match.group(1))
                timing_info.user_time = float(timing_match.group(2))
                timing_info.system_time = float(timing_match.group(3))
            
            # Parse memory information
            # Format: "Memory Used: 2382384 (max 28582800) bytes"
            memory_used_match = re.search(r'Memory Used:\s+([\d]+)', stats_content)
            if memory_used_match:
                memory_info.memory_used = int(memory_used_match.group(1))
            
            # Format: "Pager Heap Usage: 2103296 bytes"
            heap_usage_match = re.search(r'Pager Heap Usage:\s+([\d]+)', stats_content)
            if heap_usage_match:
                memory_info.heap_usage = int(heap_usage_match.group(1))
            
            # Format: "Page cache hits: 2"
            cache_hits_match = re.search(r'Page cache hits:\s+([\d]+)', stats_content)
            if cache_hits_match:
                memory_info.page_cache_hits = int(cache_hits_match.group(1))
            
            # Format: "Page cache misses: 192795"
            cache_misses_match = re.search(r'Page cache misses:\s+([\d]+)', stats_content)
            if cache_misses_match:
                memory_info.page_cache_misses = int(cache_misses_match.group(1))
            
            # Calculate effective page cache size (hits + misses)
            if memory_info.page_cache_hits is not None and memory_info.page_cache_misses is not None:
                memory_info.page_cache_size = memory_info.page_cache_hits + memory_info.page_cache_misses
            
        except Exception as e:
            print(f"Warning: Could not parse {stdout_file}: {e}")
        
        return output_rows, timing_info, memory_info

if __name__ == "__main__":
    log_path = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/test/results"
    parser = SqliteLogParser(log_path=log_path)
    metrics = parser.parse_log()
    print(metrics)
