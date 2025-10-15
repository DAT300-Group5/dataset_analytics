#!/usr/bin/env python3
import subprocess
from pathlib import Path

from consts.RunMode import RunMode
from util.file_utils import prepare_profiling_duckdb_sql_file, prepare_validate_duckdb_sql_file
from util.log_config import setup_logger

logger = setup_logger(__name__)


class DuckdbRunner:

    def __init__(self, sql_file: str, db_file: str, cmd: str = "duckdb", cwd: str = None, run_mode : RunMode = RunMode.PROFILE):

        self.sql_file = Path(sql_file)
        self.db_file = Path(db_file)
        self.cmd = cmd
        self.execution_result = None
        self.cpu_result = None
        self.cwd = Path.cwd() if cwd is None else Path(cwd)
        self.results_dir = self.cwd / "results"
        
        # Create results directory if it doesn't exist
        self.results_dir.mkdir(parents=True, exist_ok=True)

        if run_mode == RunMode.PROFILE:
            prepare_profiling_duckdb_sql_file(sql_file)
        elif run_mode == RunMode.VALIDATE:
            prepare_validate_duckdb_sql_file(sql_file)

    def run_subprocess(self):
        results_dir = Path(self.cwd) / "results"
        stdout_path = results_dir / "stdout.log"
        stderr_path = results_dir / "stderr.log"
        logger.debug(f"Running DuckDB: {self.sql_file.name} on {self.db_file.name}")
        try:
            with open(self.sql_file, 'r') as sql_input, \
                 open(stdout_path, 'w') as stdout_file, \
                    open(stderr_path, 'w') as stderr_file:
                process = subprocess.Popen(
                    [self.cmd, str(self.db_file)],
                    stdin=sql_input,
                    stdout=stdout_file,
                    stderr=stderr_file,
                    cwd=self.cwd,
                    text=True
                )
                return process
        except Exception as e:
            logger.error(f"Execution failed: {e}")
            raise


if __name__ == "__main__":
    sql_file = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/queries/Q1/Q1_duckdb.sql"
    db_file = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/db_vs14/vs14_data.duckdb"
    cwd = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/test"
    runner = DuckdbRunner(sql_file=sql_file, db_file=db_file, cwd=cwd)
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