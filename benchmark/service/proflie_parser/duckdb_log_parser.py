from pathlib import Path
import json
import re

from benchmark.service.proflie_parser.query_metric import QueryMetrics, TimingInfo, MemoryInfo


class DuckdbLogParser:
    def __init__(self, log_path):
        self.log_path = Path(log_path)

    def parse_log(self) -> QueryMetrics:
        """Parse DuckDB log files and extract metrics."""
        stdout_file = self.log_path / "stdout.log"
        
        if not stdout_file.exists():
            raise FileNotFoundError(f"Log file {stdout_file} does not exist.")
        
        # Parse stdout for row count
        output_rows = self._parse_stdout_rows(stdout_file)
        
        # Parse profiling JSON files for timing and memory info
        timing_info, memory_info, query_count = self._parse_profiling_files()
        
        return QueryMetrics(
            query_count=query_count,
            timing=timing_info,
            memory=memory_info,
            output_rows=output_rows
        )
    
    def _parse_stdout_rows(self, stdout_file: Path) -> int:
        """Parse stdout.log to extract the row count from the last query output.
        
        Looks for patterns like: "25051 rows (40 shown)"
        """
        output_rows = 0
        
        try:
            with open(stdout_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Find all occurrences of "X rows" pattern
            # Pattern: "25051 rows (40 shown)" or "25051 rows" or similar
            row_patterns = re.findall(r'(\d+)\s+rows?\s*(?:\(.*?\))?', content, re.IGNORECASE)
            
            if row_patterns:
                # Take the last occurrence (last query's output)
                output_rows = int(row_patterns[-1])
            
        except Exception as e:
            print(f"Warning: Could not parse row count from {stdout_file}: {e}")
        
        return output_rows
    
    def _parse_profiling_files(self) -> tuple[TimingInfo, MemoryInfo, int]:
        """Parse all profiling_query_*.json files in the log directory."""
        timing_info = TimingInfo()
        memory_info = MemoryInfo()
        query_count = 0
        
        # Find all profiling JSON files
        profiling_files = sorted(self.log_path.glob("profiling_query_*.json"))
        query_count = len(profiling_files)
        
        if not profiling_files:
            print(f"Warning: No profiling files found in {self.log_path}")
            return timing_info, memory_info, query_count
        
        total_latency = 0.0
        max_memory = 0
        
        try:
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
            print(f"Warning: Could not parse profiling files: {e}")
        
        return timing_info, memory_info, query_count

if __name__ == "__main__":
    log_path = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/test/results"
    parser = DuckdbLogParser(log_path=log_path)
    metrics = parser.parse_log()
    print(metrics)