"""
Configuration manager for benchmark experiments.

This module provides the BenchmarkConfig class for loading and validating
benchmark configuration from YAML files.
"""
from pathlib import Path
from typing import List

import yaml

from config.benchmark_config import BenchmarkConfig
from config.dataset import Dataset
from config.query_group import QueryGroup
from consts.EngineType import EngineType
from models.experiment_params import ExperimentParams


class ConfigLoader:

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config_data = self._load_config()

    def _load_config(self) -> BenchmarkConfig:
        """
        Load and parse benchmark configuration from YAML file.
        
        Returns:
            BenchmarkConfig: Configured benchmark configuration instance
        """
        # Load YAML file
        with open(self.config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        
        # Create BenchmarkConfig instance
        config = BenchmarkConfig()
        
        # Parse engines
        config.engines = [EngineType(engine) for engine in data["engines"]]
        
        # Parse simple fields
        config.repeat_pilot = data["repeat_pilot"]
        config.sample_count = data["sample_count"]
        config.std_repeat = data["std_repeat"]
        config.cwd = data["output_cwd"]
        config.compare_pairs = []
        config.validate_pairs = []

        if "compare_pairs" in data:
            for query_group, engine in data["compare_pairs"]:
                config.compare_pairs.append((query_group, EngineType(engine)))

        if "validate_pairs" in data:
            for query_group, engine in data["validate_pairs"]:
                config.validate_pairs.append((query_group, EngineType(engine)))

        # Parse engine paths
        config.engine_paths = data.get("engine_paths", {})
        
        # Parse datasets
        config.datasets = [Dataset(**ds) for ds in data["datasets"]]
        
        # Parse query groups
        config.query_groups = [QueryGroup(**qg) for qg in data["query_groups"]]
        
        return config

    def get_experiments(self) -> List[ExperimentParams]:
        """
        Generate a list of ExperimentParams for all combinations of datasets,
        query groups, and engines.

        Returns:
            List[ExperimentParams]: List of experiment parameters
        """
        experiments = []

        for dataset in self.config_data.datasets:
            for query_group in self.config_data.query_groups:
                for engine in self.config_data.engines:
                    sql_file = getattr(query_group, f"{engine.value}_sql")
                    db_file = getattr(dataset, f"{engine.value}_db") or getattr(dataset, f"{engine.value}_db_dir")

                    if sql_file and db_file:
                        # Generate experiment name as query_id + engine
                        exp_name = f"{query_group.id}_{engine.value}"
                        
                        # Get engine command from engine_paths or use engine name as default
                        engine_cmd = self.config_data.engine_paths.get(engine.value, engine.value)
                        
                        exp_params = ExperimentParams(
                            engine=engine,
                            sql_file=Path(sql_file),
                            db_file=Path(db_file),
                            exp_name=exp_name,
                            group_id=query_group.id,
                            engine_cmd=engine_cmd,
                            cwd=Path(self.config_data.cwd),
                            sample_count=self.config_data.sample_count,
                            std_repeat=self.config_data.std_repeat
                        )
                        experiments.append(exp_params)
                        print(f"Created experiment", exp_params)
        return experiments
    
if __name__ == "__main__":

    config = ConfigLoader(Path("/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/config.yaml"))
    config.get_experiments()
    print(config.config_data)