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

    def __str__(self):
        return (f"ExperimentParams(\n"
                f"  sql_file={self.sql_file.resolve()},\n"
                f"  db_file={self.db_file.resolve()},\n"
                f"  exp_name={self.exp_name},\n"
                f"  engine_cmd={self.engine_cmd},\n"
                f"  cwd={self.cwd.resolve()},\n"
                f"  sample_count={self.sample_count},\n"
                f"  std_repeat={self.std_repeat}\n"
                f")")