#!/usr/bin/env python3
"""
Benchmark experiment runner for database performance testing.

This module orchestrates benchmark experiments across multiple database engines,
datasets, and query configurations.
"""
import cmd
from pathlib import Path
from benchmark.config.config_loader import ConfigLoader
from benchmark.consts.EngineType import EngineType
from benchmark.models.experiment_params import ExperimentParams
from benchmark.service.proflie_parser.duckdb_log_parser import DuckdbLogParser
from benchmark.service.proflie_parser.sqlite_log_parser import SqliteLogParser
from benchmark.service.runner.sqlite_runner import SQLiteRunner
from benchmark.service.runner.duckdb_runner import DuckdbRunner
from benchmark.service.task_executor.task_executor import TaskExecutor


def build_experiment(params : ExperimentParams) :
    sql_file = str(params.sql_file.resolve())
    db_file = str(params.db_file.resolve())
    engine_cmd = params.engine_cmd
    cwd = str((params.cwd / params.exp_name).resolve())
    if params.engine == EngineType.SQLITE:
        runner = SQLiteRunner(sql_file=sql_file, db_file=db_file, cmd=engine_cmd, cwd=cwd)
        sqlite_parser = SqliteLogParser(log_path=runner.results_dir)
        task_executor = TaskExecutor(runner=runner, log_parser=sqlite_parser, sample_count=params.sample_count, std_repeat=params.std_repeat)
        return task_executor
    elif params.engine == EngineType.DUCKDB:
        runner = DuckdbRunner(sql_file=sql_file, db_file=db_file, cmd=engine_cmd, cwd=cwd)
        duckdb_parser = DuckdbLogParser(log_path=runner.results_dir)
        task_executor = TaskExecutor(runner=runner, log_parser=duckdb_parser, sample_count=params.sample_count, std_repeat=params.std_repeat)
        return task_executor

def main() -> None:
    """
    Main entry point for benchmark experiment execution.
    
    Orchestrates the complete benchmark workflow:
    1. Load configuration
    2. Run pilot tests for all combinations to determine intervals
    3. Choose unified intervals per (dataset, query_group)
    4. Execute full benchmark runs with unified intervals
    5. Collect and export results to CSV manifest
    """
    # Initialize components
    # Get config path relative to this file
    config_path = Path(__file__).parent / "config.yaml"
    config = ConfigLoader(config_path)
    experiments = config.get_experiments()
    print(f"Experiments: {len(experiments)}")
    
    # Print experiment details
    for exp in experiments:
        task_executor = build_experiment(exp)
        print(f"Experiment: {exp}, TaskExecutor: {task_executor}")
        task_executor.std_execute()

if __name__ == "__main__":
    main()
