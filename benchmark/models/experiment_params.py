from dataclasses import dataclass


@dataclass
class ExperimentParams:
    sql_file : str
    db_file : str
    engine_cmd : str
    cwd : str
    sample_count : int
    std_repeat : int