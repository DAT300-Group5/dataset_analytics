#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path

from consts.RunMode import RunMode
from .runner import Runner
from util.file_utils import resolve_cmd
from util.log_config import setup_logger


logger = setup_logger(__name__)


class ChdbRunner(Runner):

    def __init__(self, sql_file: Path, db_file: Path, cmd: str = "chdb", cwd: Path = None, run_mode: RunMode = RunMode.PROFILE):

        self.sql_file = sql_file
        self.db_file = db_file
        self.cmd = cmd
        self.cwd = Path.cwd() if cwd is None else cwd
        self.run_mode = run_mode
        
        self.results_dir = self.cwd / str(run_mode.name)
        # Create results directory if it doesn't exist
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        self.library_path = None

    def set_library_path(self, library_path: str):
        self.library_path = library_path

    def run_subprocess(self) -> subprocess.Popen:

        output_path = self.results_dir / "stdout.log"
        stderr_path = self.results_dir / "stderr.log"
        if self.run_mode == RunMode.VALIDATE:
            output_path = self.results_dir / "result.csv"

        logger.debug(f"Running chDB: {self.sql_file.name} on {self.db_file.name}")
        
        env = os.environ.copy()
        # Add or modify library path if needed
        if self.library_path:
            env['DYLD_LIBRARY_PATH'] = '/usr/local/lib'

        try:
            with open(self.sql_file, 'r') as sql_input, \
                    open(output_path, 'w') as output_file, \
                        open(stderr_path, 'w') as stderr_file:
                
                # chDB outputs in CSV format by default
                cmd_args = [resolve_cmd(self.cmd), str(self.db_file)]
                if self.run_mode == RunMode.PROFILE:
                    cmd_args.append('-v')
                    cmd_args.append('-m')
                
                process = subprocess.Popen(
                        cmd_args,
                        stdin=sql_input,
                        stdout=output_file,
                        stderr=stderr_file,
                        cwd=self.results_dir,
                        text=True,
                        env=env
                    )
                return process
        except Exception as e:
            logger.error(f"Execution failed: {e}")
            raise


if __name__ == "__main__":
    
    # python3 -m service.runner.chdb_runner

    from util.file_utils import project_root
    
    root = project_root()

    chdb_cmd = root / "benchmark/chdb_cli/chdb_cli"
    sql_file = root / "benchmark/queries/Q1/Q1_chdb.sql"
    db_file = root / "benchmark/db_vs14/vs14_data_chdb"
    cwd = root / "benchmark/test"

    runner = ChdbRunner(sql_file=sql_file, db_file=db_file, cwd=cwd, cmd=str(chdb_cmd), run_mode=RunMode.PROFILE)

    process = runner.run_subprocess()
    stdout, stderr = process.communicate()
    if process.returncode == 0:
        logger.info("Execution succeeded")
        if stdout:
            logger.debug(stdout)
    else:
        logger.error("Execution failed")
        if stderr:
            logger.error(stderr)
