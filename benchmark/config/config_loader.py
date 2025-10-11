"""
Configuration manager for benchmark experiments.

This module provides the BenchmarkConfig class for loading and validating
benchmark configuration from YAML files.
"""
from typing import List

import yaml

from benchmark.config.benchmark_config import BenchmarkConfig
from benchmark.config.dataset import Dataset
from benchmark.config.query_group import QueryGroup
from benchmark.consts.EngineType import EngineType
from benchmark.models.experiment_params import ExperimentParams


class ConfigLoader:

    def __init__(self, config_path: str):
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
        config.cwd = data["cwd"]
        
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
                        exp_params = ExperimentParams(
                            sql_file=sql_file,
                            db_file=db_file,
                            engine_cmd=engine.value,
                            cwd=self.config_data.cwd,
                            sample_count=self.config_data.sample_count,
                            std_repeat=self.config_data.std_repeat
                        )
                        experiments.append(exp_params)

        return experiments
    
if __name__ == "__main__":

    config = ConfigLoader("/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/config.yaml")
    config.get_experiments()
    print(config.config_data)