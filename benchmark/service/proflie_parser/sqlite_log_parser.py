from pathlib import Path
import re

from service.proflie_parser.query_metric import QueryMetrics, TimingInfo, MemoryInfo
from util.log_config import setup_logger

logger = setup_logger(__name__)


class SqliteLogParser:
    def __init__(self, log_path):
        self.log_path = Path(log_path)

    def parse_log(self) -> QueryMetrics:
        """Parse SQLite log files and extract metrics."""
        stdout_file = self.log_path / "stdout.log"
        
        if not stdout_file.exists():
            raise FileNotFoundError(f"Log file {stdout_file} does not exist.")
        
        # Parse stdout file - it contains both data rows and statistics
        output_rows, timing_info, memory_info, query_count = self._parse_stdout(stdout_file)
        
        return QueryMetrics(
            query_count=query_count,
            timing=timing_info,
            memory=memory_info,
            output_rows=output_rows
        )
    
    def _parse_stdout(self, stdout_file: Path) -> tuple[int, TimingInfo, MemoryInfo, int]:
        """Parse stdout.log which contains both data rows and statistics."""
        timing_info = TimingInfo()
        memory_info = MemoryInfo()
        output_rows = 0
        query_count = 0
        
        try:
            with open(stdout_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Find all statistics section boundaries
            # Statistics section starts with lines like "Memory Used:" or "Run Time:"
            stats_indices = []
            for i, line in enumerate(lines):
                if re.match(r'^(Memory Used:|Run Time:|Number of|Largest|Lookaside|Pager|Page cache|Schema|Statement|Fullscan|Sort|Autoindex|Bloom|Virtual|Reprepare)', line):
                    stats_indices.append(i)
            
            # If there are statistics, find the start of the last statistics section
            if stats_indices:
                # Find the last contiguous block of statistics (last query's stats)
                last_stats_start = stats_indices[-1]
                # Look backwards to find where this stats block starts
                for i in range(len(stats_indices) - 1, -1, -1):
                    if i == 0 or stats_indices[i] - stats_indices[i-1] > 5:
                        # Found the beginning of the last stats block
                        last_stats_start = stats_indices[i]
                        break
                
                # Count output rows from the last query (between previous stats end and last stats start)
                # Find the end of the previous stats block (if exists)
                if len(stats_indices) > 1:
                    # Find where previous stats block ends by looking for the last stats line before a gap
                    prev_stats_end = 0
                    for i in range(len(stats_indices) - 1):
                        if stats_indices[i+1] - stats_indices[i] > 5:
                            # Found a gap, previous stats ends here
                            prev_stats_end = stats_indices[i] + 20  # Approximate end of stats block
                            break
                    
                    # Count rows between previous stats end and last stats start
                    output_rows = sum(1 for line in lines[prev_stats_end:last_stats_start] if line.strip())
                else:
                    # Only one query, count all rows before stats
                    output_rows = sum(1 for line in lines[:last_stats_start] if line.strip())
                
                stats_start_idx = stats_indices[0]
            else:
                # No statistics found, count all lines
                output_rows = sum(1 for line in lines if line.strip())
                stats_start_idx = len(lines)
            
            # Parse statistics section
            stats_content = ''.join(lines[stats_start_idx:])
            
            # Parse timing information and count queries
            # Format: "Run Time: real 17.338 user 14.308602 sys 2.313507"
            timing_matches = re.findall(r'Run Time: real\s+([\d.]+)\s+user\s+([\d.]+)\s+sys\s+([\d.]+)', stats_content)
            query_count = len(timing_matches)
            
            if timing_matches:
                # Sum up all timing results for multiple queries
                total_run_time = sum(float(match[0]) for match in timing_matches)
                total_user_time = sum(float(match[1]) for match in timing_matches)
                total_system_time = sum(float(match[2]) for match in timing_matches)
                
                timing_info.run_time = total_run_time
                timing_info.user_time = total_user_time
                timing_info.system_time = total_system_time
            
            # Parse memory information
            # Format: "Memory Used: 2382384 (max 28582800) bytes"
            # Find all occurrences and take the maximum max_memory_used
            memory_used_matches = re.findall(r'Memory Used:\s+([\d]+)\s+\(max\s+([\d]+)\)', stats_content)
            if memory_used_matches:
                # Use the last memory_used value
                memory_info.memory_used = int(memory_used_matches[-1][0])
                # Take the maximum of all max_memory_used values
                memory_info.max_memory_used = max(int(match[1]) for match in memory_used_matches)
            else:
                # Fallback: try without max value
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
            logger.warning(f"Could not parse {stdout_file.name}: {e}")
        
        return output_rows, timing_info, memory_info, query_count

if __name__ == "__main__":
    
    # python3 -m service.proflie_parser.sqlite_log_parser

    from util.file_utils import project_root
    
    root = project_root()
    
    # need sqlite log files in test directory
    log_path = root / "benchmark/test/"
    
    parser = SqliteLogParser(log_path=log_path)
    metrics = parser.parse_log()
    logger.info(f"Parsed metrics: {metrics}")
