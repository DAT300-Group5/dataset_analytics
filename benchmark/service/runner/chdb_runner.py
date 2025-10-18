#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path

from consts.RunMode import RunMode
from util.log_config import setup_logger

logger = setup_logger(__name__)


class ChdbRunner:

    def __init__(self, sql_file: str, db_file: str, cmd: str = "chdb", cwd: str = None, run_mode : RunMode = RunMode.PROFILE):

        self.original_sql_file = Path(sql_file)
        self.db_file = Path(db_file)
        self.cmd = cmd
        self.execution_result = None
        self.cpu_result = None
        self.cwd = Path.cwd() if cwd is None else Path(cwd)
        self.results_dir = self.cwd / "results"
        self.sql_file = Path(sql_file)
        
        # Create results directory if it doesn't exist
        self.results_dir.mkdir(parents=True, exist_ok=True)

        self.enable_profiling = (run_mode == RunMode.PROFILE)

        self.library_path = None

    def set_library_path(self, library_path: str):
        self.library_path = library_path

    def run_subprocess(self):
        results_dir = Path(self.cwd) / "results"
        stdout_path = results_dir / "stdout.log"
        stderr_path = results_dir / "stderr.log"
        output_path = Path(self.cwd) / "result.csv"
        logger.debug(f"Running chdb: {self.sql_file.name} on {self.db_file.name}")
        env = os.environ.copy()

        # Add or modify library path if needed
        if self.library_path:
            env['DYLD_LIBRARY_PATH'] = '/usr/local/lib'

        try:
            with open(self.sql_file, 'r') as sql_input, \
                 open(stdout_path, 'w') as stdout_file, \
                    open(stderr_path, 'w') as stderr_file, \
                        open(output_path, 'w') as output_file:
                cmd_args = [Path(self.cmd).resolve(), str(self.db_file)]

                if self.enable_profiling:
                    cmd_args.append('-v')
                    cmd_args.append('-m')
                    process = subprocess.Popen(
                        cmd_args,
                        stdin=sql_input,
                        stdout=stdout_file,
                        stderr=stderr_file,
                        cwd=self.cwd,
                        text=True,
                        env=env
                    )
                    return process
                else:
                    process = subprocess.Popen(
                        cmd_args,
                        stdin=sql_input,
                        stdout=output_file,
                        stderr=stderr_file,
                        cwd=self.cwd,
                        text=True,
                        env=env
                    )
                    return process
        except Exception as e:
            logger.error(f"Execution failed: {e}")
            raise


if __name__ == "__main__":
    sql_file = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/queries/Q1/Q1_clickhouse.sql"
    db_file = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/db_vs14/vs14_data_chdb"
    cwd = "/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/test"
    runner = ChdbRunner(sql_file=sql_file, db_file=db_file, cwd=cwd, cmd="/Users/xiejiangzhao/tmp/chdb_cli", run_mode=RunMode.PROFILE)

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

