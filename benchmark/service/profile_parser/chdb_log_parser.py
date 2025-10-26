from pathlib import Path
import re

from service.profile_parser.query_metric import QueryMetrics, TimingInfo, MemoryInfo
from util.log_config import setup_logger

logger = setup_logger(__name__)


class ChdbLogParser:
    def __init__(self, log_path: Path):
        self.log_path = log_path

    def parse_log(self) -> QueryMetrics:
        """Parse chdb log files and extract metrics."""
        stdout_file = self.log_path / "stdout.log"
        
        if not stdout_file.exists():
            raise FileNotFoundError(f"Log file {stdout_file} does not exist.")
        
        # Parse stdout file - it contains query statistics
        timing_info, memory_info, output_rows = self._parse_stdout(stdout_file)
        
        return QueryMetrics(
            timing=timing_info,
            memory=memory_info,
            output_rows=output_rows
        )
    
    def _parse_stdout(self, stdout_file: Path) -> tuple[TimingInfo, MemoryInfo, int]:
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

            
        except Exception as e:
            logger.warning(f"Could not parse {stdout_file.name}: {e}")
        
        return timing_info, memory_info, output_rows


if __name__ == "__main__":

    # python3 -m service.profile_parser.chdb_log_parser

    from util.file_utils import project_root
    
    root = project_root()
    
    # need chDB log files in test directory
    log_path = root / "benchmark/test/"
    
    parser = ChdbLogParser(log_path=log_path)
    metrics = parser.parse_log()
    logger.info(f"Parsed metrics: {metrics}")