from dataclasses import dataclass
from pathlib import Path


@dataclass
class ExperimentParams:
    sql_file : Path
    db_file : Path
    exp_name : str
    engine_cmd : str
    cwd : Path
    sample_count : int
    std_repeat : int