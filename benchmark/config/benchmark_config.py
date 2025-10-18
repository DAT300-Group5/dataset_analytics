from typing import Dict, List, Tuple

from config.dataset import Dataset
from config.query_group import QueryGroup
from consts.EngineType import EngineType


class BenchmarkConfig:
    engines : List[EngineType]
    repeat_pilot: int
    sample_count: int
    std_repeat: int
    cwd: str
    engine_paths: Dict[str, str]  # Map engine name to executable path
    chdb_library_path: str
    datasets: List[Dataset]
    query_groups: List[QueryGroup]
    compare_pairs: List[Tuple[str, EngineType]]
    validate_pairs: List[Tuple[str, EngineType]]
