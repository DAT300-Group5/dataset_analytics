from typing import List

from benchmark.config.dataset import Dataset
from benchmark.config.query_group import QueryGroup
from benchmark.consts.EngineType import EngineType


class BenchmarkConfig:
    engines : List[EngineType]
    repeat_pilot: int
    sample_count: int
    std_repeat: int
    datasets: List[Dataset]
    query_groups: List[QueryGroup]
