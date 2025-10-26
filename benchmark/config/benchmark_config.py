from typing import Dict, List, Optional

from config.dataset import Dataset
from config.execution_unit import ExecutionUnit
from config.query_group import QueryGroup
from consts.EngineType import EngineType


class BenchmarkConfig:
    engines : List[EngineType]
    repeat_pilot: int
    sample_count: int
    std_repeat: int
    cwd: str
    engine_paths: Dict[str, str]  # Map engine name to executable path
    chdb_library_path: Optional[str]
    datasets: List[Dataset]
    query_groups: List[QueryGroup]
    execute_pairs: List[ExecutionUnit]
    compare_pairs: List[ExecutionUnit]
    validate_pairs: List[ExecutionUnit]
