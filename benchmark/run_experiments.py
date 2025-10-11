#!/usr/bin/env python3
"""
Benchmark experiment runner for database performance testing.

This module orchestrates benchmark experiments across multiple database engines,
datasets, and query configurations.
"""
import json
from pathlib import Path

from benchmark.config.config_loader import ConfigLoader
from benchmark.consts.EngineType import EngineType
from benchmark.models.experiment_params import ExperimentParams
from benchmark.service.proflie_parser.duckdb_log_parser import DuckdbLogParser
from benchmark.service.proflie_parser.sqlite_log_parser import SqliteLogParser
from benchmark.service.runner.duckdb_runner import DuckdbRunner
from benchmark.service.runner.sqlite_runner import SQLiteRunner
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

def add_result_to_summary(summary: dict, group_id: str, engine: EngineType, result: dict) -> None:
    if group_id not in summary:
        summary[group_id] = {}
    summary[group_id][engine] = result

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

    summary = {}
    # Print experiment details
    for exp in experiments:
        task_executor = build_experiment(exp)
        print(f"Experiment: {exp}, TaskExecutor: {task_executor}")
        result = task_executor.std_execute()
        add_result_to_summary(summary, exp.group_id, exp.engine, result.to_dict())

    # Export summary to CSV
    summary_path = Path(config.config_data.cwd) / "summary.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"Summary exported to {summary_path.resolve()}")

if __name__ == "__main__":
    main()
