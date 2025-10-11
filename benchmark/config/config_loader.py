"""
Configuration manager for benchmark experiments.

This module provides the BenchmarkConfig class for loading and validating
benchmark configuration from YAML files.
"""
import yaml

from benchmark.config.benchmark_config import BenchmarkConfig
from benchmark.config.dataset import Dataset
from benchmark.config.query_group import QueryGroup
from benchmark.consts.EngineType import EngineType


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
    
if __name__ == "__main__":

    config = ConfigLoader("/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/config.yaml")
    print(config.config_data)