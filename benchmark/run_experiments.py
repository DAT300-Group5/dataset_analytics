#!/usr/bin/env python3
"""
Benchmark experiment runner for database performance testing.

This module orchestrates benchmark experiments across multiple database engines,
datasets, and query configurations.
"""
import json
from pathlib import Path

from config.config_loader import ConfigLoader
from consts.EngineType import EngineType
from models.experiment_params import ExperimentParams
from service.profile_parser.chdb_log_parser import ChdbLogParser
from service.profile_parser.duckdb_log_parser import DuckdbLogParser
from service.profile_parser.sqlite_log_parser import SqliteLogParser
from service.runner.chdb_runner import ChdbRunner
from service.runner.duckdb_runner import DuckdbRunner
from service.runner.sqlite_runner import SQLiteRunner
from service.task_executor.task_executor import TaskExecutor
from cli.cli import parse_env_args
from util.log_config import setup_logger

logger = setup_logger(__name__)


def build_experiment(params : ExperimentParams) -> TaskExecutor:
    sql_file = params.sql_file.resolve()
    db_file = params.db_file.resolve()
    engine_cmd = params.engine_cmd

    cwd = (params.cwd / params.db_name / params.exp_name).resolve()

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
    elif params.engine == EngineType.CHDB:
        runner = ChdbRunner(sql_file=sql_file, db_file=db_file, cmd=engine_cmd, cwd=cwd)
        if params.chdb_library_path is not None:
            runner.set_library_path(params.chdb_library_path)
        chdb_parser = ChdbLogParser(log_path=runner.results_dir)
        task_executor = TaskExecutor(runner=runner, log_parser=chdb_parser, sample_count=params.sample_count, std_repeat=params.std_repeat)
        return task_executor

    # Ensure we never return None; signal unsupported engine explicitly.
    raise ValueError(f"Unsupported engine type: {params.engine}")

def add_result_to_dict(data_dict: dict, exp: ExperimentParams, result: dict) -> None:
    group_id = exp.group_id
    db_name = exp.db_name
    optimizer_status = "ban_ops" if exp.ban_optimizer else "default"
    data_dict.setdefault(db_name, {}).setdefault(group_id, {}).setdefault(exp.engine.value, {})[optimizer_status] = result

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
    # Parse command line arguments
    args = parse_env_args("Run benchmark experiments for database performance testing")
    
    # Initialize components
    logger.info("=" * 60)
    logger.info("Starting Benchmark Experiments")
    logger.info("=" * 60)
    
    # Get config path relative to this file
    config_path = Path(__file__).parent / "config_yaml"
    
    # Load configuration with optional environment override
    config = ConfigLoader(config_path, env=args.env)
    
    if args.env:
        logger.info(f"Loaded configuration with environment override: {args.env}")
    
    experiments = config.filter_experiments(config.config_data.execute_pairs, True)
    logger.info(f"Loaded {len(experiments)} experiments from config")
    logger.info("")

    summary = {}
    raw_data = {}
    # Execute experiments
    for idx, exp in enumerate(experiments, 1):
        logger.info("-" * 60)
        logger.info(f"Experiment {idx}/{len(experiments)}: {exp.group_id} ({exp.engine.value})")
        logger.info("-" * 60)
        task_executor = build_experiment(exp)
        result = task_executor.std_execute()
        add_result_to_dict(summary, exp, result.to_summary_dict())
        add_result_to_dict(raw_data, exp, result.to_raw_data_dict())
        logger.info(f"✓ Experiment {idx}/{len(experiments)} completed")
        logger.info("")

    # Export summary to JSON
    logger.info("=" * 60)
    logger.info("Exporting Results")
    logger.info("=" * 60)
    for db_name in summary.keys():
        summary_path = Path(config.config_data.cwd) / db_name / "summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary[db_name], f, indent=2)
        logger.info(f"✓ Summary results exported to: {summary_path.resolve()}")
        logger.info("")
        raw_data_path = Path(config.config_data.cwd) / db_name / "raw_data.json"
        with open(raw_data_path, 'w') as f:
            json.dump(raw_data[db_name], f, indent=2)
        logger.info(f"✓ Raw data exported to: {raw_data_path.resolve()}")
        logger.info("")
    logger.info("All experiments completed successfully!")

if __name__ == "__main__":
    main()
