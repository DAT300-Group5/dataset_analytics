#!/usr/bin/env python3
import subprocess
from pathlib import Path

from consts.RunMode import RunMode
from .runner import Runner
from util.file_utils import resolve_cmd
from util.log_config import setup_logger

logger = setup_logger(__name__)


class SQLiteRunner(Runner):

    def __init__(self, sql_file: Path, db_file: Path, cwd: Path = Path.cwd(), cmd: str = "sqlite", run_mode: RunMode = RunMode.PROFILE):
        
        super().__init__(
            sql_file,
            db_file,
            cmd,
            cwd,
            run_mode,
            cwd / str(run_mode.name)
        )
        
        # Create results directory if it doesn't exist
        self.results_dir.mkdir(parents=True, exist_ok=True)

    def _run_subprocess(self) -> subprocess.Popen:

        output_path = self.results_dir / "stdout.log"
        stderr_path = self.results_dir / "stderr.log"
        if self.run_mode == RunMode.VALIDATE:
            output_path = self.results_dir / "result.csv"

        logger.debug(f"Running SQLite: {self.sql_file.name} on {self.temp_db_file.name}")
        
        try:
            with open(self.sql_file, 'r') as sql_input, \
                    open(output_path, 'w') as output_file, \
                        open(stderr_path, 'w') as stderr_file:
                
                # always output in CSV format with header
                cmd_args = [
                    resolve_cmd(self.cmd),
                    str(self.temp_db_file),
                    '-csv', '-header'
                ]
                if self.run_mode == RunMode.PROFILE:
                    # assume no dot commands in sql file
                    cmd_args += ['-init', str(Path(__file__).parent / '.sqliterc')]
                
                process = subprocess.Popen(
                    cmd_args,
                    stdin=sql_input,
                    stdout=output_file,
                    stderr=stderr_file,
                    cwd=self.results_dir,
                    text=True
                )
                return process
        except Exception as e:
            logger.error(f"Execution failed: {e}")
            raise


if __name__ == "__main__":

    # python3 -m service.runner.sqlite_runner

    from util.file_utils import project_root
    
    root = project_root()

    sqlite_cmd = "sqlite3"
    sql_file = root / "benchmark/queries/Q1/Q1_sqlite.sql"
    db_file = root / "benchmark/db_vs14/vs14_data.sqlite"
    cwd = root / "benchmark/test"

    runner = SQLiteRunner(sql_file=sql_file, db_file=db_file, cmd=sqlite_cmd, cwd=cwd, run_mode=RunMode.PROFILE)
    
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