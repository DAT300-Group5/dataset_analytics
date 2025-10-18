from pathlib import Path
import re

from service.proflie_parser.query_metric import QueryMetrics, TimingInfo, MemoryInfo
from util.log_config import setup_logger

logger = setup_logger(__name__)


class ChdbLogParser:
    def __init__(self, log_path):
        self.log_path = Path(log_path)

    def parse_log(self) -> QueryMetrics:
        """Parse chdb log files and extract metrics."""
        stdout_file = self.log_path / "stdout.log"
        
        if not stdout_file.exists():
            raise FileNotFoundError(f"Log file {stdout_file} does not exist.")
        
        # Parse stdout file - it contains query statistics
        timing_info, memory_info, output_rows, query_count = self._parse_stdout(stdout_file)
        
        return QueryMetrics(
            query_count=query_count,
            timing=timing_info,
            memory=memory_info,
            output_rows=output_rows
        )
    
    def _parse_stdout(self, stdout_file: Path) -> tuple[TimingInfo, MemoryInfo, int, int]:
        """Parse stdout.log which contains query statistics.
        
        Expected format:
        Query statistics:
          Elapsed: 0.768 seconds
          Output rows: 25031
        Peak memory: 387.172 MB
        Query count: 3
        [CSV data follows...]
        """
        timing_info = TimingInfo()
        memory_info = MemoryInfo()
        output_rows = 0
        query_count = 1  # Default to 1 if not found
        
        try:
            with open(stdout_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Parse elapsed time
            # Format: "Elapsed: 0.768 seconds"
            elapsed_match = re.search(r'Elapsed:\s+([\d.]+)\s+seconds', content)
            if elapsed_match:
                timing_info.run_time = float(elapsed_match.group(1))
            else:
                logger.warning("Could not find 'Elapsed' in stdout")
            
            # Parse output rows
            # Format: "Output rows: 25031"
            rows_match = re.search(r'Output rows:\s+([\d]+)', content)
            if rows_match:
                output_rows = int(rows_match.group(1))
            else:
                logger.warning("Could not find 'Output rows' in stdout")
            
            # Parse peak memory
            # Format: "Peak memory: 387.172 MB"
            memory_match = re.search(r'Peak memory:\s+([\d.]+)\s+MB', content)
            if memory_match:
                memory_mb = float(memory_match.group(1))
                # Convert MB to bytes
                memory_bytes = int(memory_mb * 1024 * 1024)
                memory_info.max_memory_used = memory_bytes
                memory_info.memory_used = memory_bytes
            else:
                logger.warning("Could not find 'Peak memory' in stdout")
            
            # Parse query count
            # Format: "Query count: 3"
            query_count_match = re.search(r'Query count:\s+([\d]+)', content)
            if query_count_match:
                query_count = int(query_count_match.group(1))
            else:
                logger.debug("Could not find 'Query count' in stdout, using default value 1")
            
        except Exception as e:
            logger.warning(f"Could not parse {stdout_file.name}: {e}")
        
        return timing_info, memory_info, output_rows, query_count


if __name__ == "__main__":
    log_path = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/test/results"
    parser = ChdbLogParser(log_path=log_path)
    metrics = parser.parse_log()
    logger.info(f"Parsed metrics: {metrics}")