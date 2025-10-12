from pathlib import Path
import json

from benchmark.service.proflie_parser.query_metric import QueryMetrics, TimingInfo, MemoryInfo
from benchmark.util.log_config import setup_logger

logger = setup_logger(__name__)


class DuckdbLogParser:
    def __init__(self, log_path):
        self.log_path = Path(log_path)

    def parse_log(self) -> QueryMetrics:
        """Parse DuckDB log files and extract metrics."""
        stdout_file = self.log_path / "stdout.log"
        
        if not stdout_file.exists():
            raise FileNotFoundError(f"Log file {stdout_file} does not exist.")
        
        # Parse profiling JSON files for timing, memory, and row count
        timing_info, memory_info, query_count, output_rows = self._parse_profiling_files()
        
        return QueryMetrics(
            query_count=query_count,
            timing=timing_info,
            memory=memory_info,
            output_rows=output_rows
        )
    
    def _parse_profiling_files(self) -> tuple[TimingInfo, MemoryInfo, int, int]:
        """Parse all profiling_query_*.json files in the log directory.
        
        Returns:
            tuple: (timing_info, memory_info, query_count, output_rows)
        """
        timing_info = TimingInfo()
        memory_info = MemoryInfo()
        query_count = 0
        output_rows = 0
        
        # Find all profiling JSON files
        profiling_files = sorted(self.log_path.glob("profiling_query_*.json"))
        query_count = len(profiling_files)
        
        if not profiling_files:
            logger.warning(f"No profiling files found in {self.log_path}")
            return timing_info, memory_info, query_count, output_rows
        
        total_latency = 0.0
        max_memory = 0
        
        try:
            # Iterate through all profiling files
            for pf in profiling_files:
                with open(pf, 'r', encoding='utf-8') as f:
                    profile_data = json.load(f)
                
                # Extract latency (run_time)
                if 'latency' in profile_data:
                    total_latency += profile_data['latency']
                
                # Extract system_peak_buffer_memory (max_memory_used)
                if 'system_peak_buffer_memory' in profile_data:
                    max_memory = max(max_memory, profile_data['system_peak_buffer_memory'])
            
            # Get rows_returned from the LAST profiling file (last query)
            last_profile_file = profiling_files[-1]
            with open(last_profile_file, 'r', encoding='utf-8') as f:
                last_profile_data = json.load(f)
                if 'rows_returned' in last_profile_data:
                    output_rows = last_profile_data['rows_returned']
                else:
                    logger.warning(f"'rows_returned' field not found in {last_profile_file.name}")
            
            # Set timing info (cumulative latency)
            if total_latency > 0:
                timing_info.run_time = total_latency
            
            # Set memory info (peak across all queries)
            if max_memory > 0:
                memory_info.max_memory_used = max_memory
                memory_info.memory_used = max_memory  # Use peak as current for consistency
            
        except Exception as e:
            logger.warning(f"Could not parse profiling files: {e}")
        
        return timing_info, memory_info, query_count, output_rows

if __name__ == "__main__":
    log_path = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/test/results"
    parser = DuckdbLogParser(log_path=log_path)
    metrics = parser.parse_log()
    logger.info(f"Parsed metrics: {metrics}")