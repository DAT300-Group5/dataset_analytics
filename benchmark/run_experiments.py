#!/usr/bin/env python3
"""
Benchmark experiment runner for database performance testing.

This module orchestrates benchmark experiments across multiple database engines,
datasets, and query configurations.
"""
from pathlib import Path
from benchmark.config.config_loader import ConfigLoader


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
        print(f"\nExperiment: {exp.exp_name}")
        print(f"  Engine: {exp.engine_cmd}")
        print(f"  SQL: {exp.sql_file}")
        print(f"  DB: {exp.db_file}")
        print(f"  CWD: {exp.cwd}")
        print(f"  Sample Count: {exp.sample_count}")
        print(f"  STD Repeat: {exp.std_repeat}")

if __name__ == "__main__":
    main()
