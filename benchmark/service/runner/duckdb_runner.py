#!/usr/bin/env python3
import subprocess
from pathlib import Path

from consts.RunMode import RunMode
from .runner import Runner
from util.file_utils import prepare_profiling_duckdb_sql_file, resolve_cmd
from util.log_config import setup_logger

logger = setup_logger(__name__)


class DuckdbRunner(Runner):

    def __init__(self, sql_file: Path, db_file: Path, cwd: Path = Path.cwd(), cmd: str = "duckdb", run_mode: RunMode = RunMode.PROFILE):

        super().__init__(
            prepare_profiling_duckdb_sql_file(sql_file) if run_mode == RunMode.PROFILE else sql_file,
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
        
        logger.debug(f"Running DuckDB: {self.sql_file.name} on {self.db_file.name}")
        
        try:
            # duckdb allows reading from file directly with -f, no need to redirect stdin
            with open(output_path, 'w') as output_file, \
                    open(stderr_path, 'w') as stderr_file:
                
                # always output in CSV format with header
                cmd_args = [
                    resolve_cmd(self.cmd), # duckdb executable
                    str(self.db_file),
                    '-no-stdin',
                    '-csv', '-header', # need to add before -f
                    '-f', str(self.sql_file),
                ]
                
                process = subprocess.Popen(
                    cmd_args,
                    stdin=None,
                    stdout=output_file,
                    stderr=stderr_file,
                    cwd=self.results_dir,  # will make profiling_query_*.json files appear here
                    text=True
                )
                return process
        except Exception as e:
            logger.error(f"Execution failed: {e}")
            raise


if __name__ == "__main__":
    
    # python3 -m service.runner.duckdb_runner

    from util.file_utils import project_root
    
    root = project_root()

    sql_file = root / "benchmark/queries/Q1/Q1_duckdb.sql"
    db_file = root / "benchmark/db_vs14/vs14_data.duckdb"
    cwd = root / "benchmark/test"

    runner = DuckdbRunner(sql_file=sql_file, db_file=db_file, cwd=cwd, run_mode=RunMode.PROFILE)

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