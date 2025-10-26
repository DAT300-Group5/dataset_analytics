from pathlib import Path
import json
import pandas as pd

from service.profile_parser.query_metric import QueryMetrics, TimingInfo, MemoryInfo
from util.log_config import setup_logger

logger = setup_logger(__name__)


class DuckdbLogParser:
    def __init__(self, log_path: Path):
        self.log_path = log_path

    def parse_log(self) -> QueryMetrics:
        """Parse DuckDB log files and extract metrics."""
        stdout_file = self.log_path / "stdout.log"
        
        if not stdout_file.exists():
            raise FileNotFoundError(f"Log file {stdout_file} does not exist.")
        
        # Get output_rows from stdout.log (CSV file)
        output_rows = self._parse_output_rows(stdout_file)
        
        # Parse profiling JSON files for timing, memory, and query count
        timing_info, memory_info = self._parse_profiling_files()
        
        return QueryMetrics(
            timing=timing_info,
            memory=memory_info,
            output_rows=output_rows
        )
    
    def _parse_output_rows(self, stdout_file: Path) -> int:
        """Parse stdout.log CSV file and count output rows (excluding header)."""
        try:
            # Read CSV and count rows (excluding header)
            df = pd.read_csv(stdout_file)
            return len(df)
        except Exception as e:
            logger.warning(f"Could not parse {stdout_file.name}: {e}")
            return 0
    
    def _parse_profiling_files(self) -> tuple[TimingInfo, MemoryInfo]:
        """Parse all profiling_query_*.json files in the log directory.
        
        Returns:
            tuple: (timing_info, memory_info)
        """
        timing_info = TimingInfo()
        memory_info = MemoryInfo()

        # Find all profiling JSON files
        profiling_files = sorted(self.log_path.glob("profiling_query_*.json"))
        
        if not profiling_files:
            logger.warning(f"No profiling files found in {self.log_path}")
            return timing_info, memory_info

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
            
            # Set timing info (cumulative latency)
            if total_latency > 0:
                timing_info.run_time = total_latency
            
            # Set memory info (peak across all queries)
            if max_memory > 0:
                memory_info.max_memory_used = max_memory
                memory_info.memory_used = max_memory  # Use peak as current for consistency
            
        except Exception as e:
            logger.warning(f"Could not parse profiling files: {e}")

        return timing_info, memory_info

if __name__ == "__main__":

    # python3 -m service.profile_parser.duckdb_log_parser

    from util.file_utils import project_root
    
    root = project_root()

    # need duckdb log files in test directory
    log_path = root / "benchmark/test/"

    parser = DuckdbLogParser(log_path=log_path)
    metrics = parser.parse_log()
    logger.info(f"Parsed metrics: {metrics}")