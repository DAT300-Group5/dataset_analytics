import subprocess
from pathlib import Path
from dataclasses import dataclass

from consts.RunMode import RunMode


@dataclass
class Runner:
    
    sql_file: Path
    db_file: Path
    cmd: str
    cwd: Path
    run_mode: RunMode
    results_dir: Path

    def run_subprocess(self) -> subprocess.Popen:
        raise NotImplementedError("Subclasses should implement this method.")
