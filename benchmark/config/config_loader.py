"""
Configuration manager for benchmark experiments.

This module provides the BenchmarkConfig class for loading and validating
benchmark configuration from YAML files.
"""
from pathlib import Path

from benchmark.config.benchmark_config import BenchmarkConfig


class ConfigLoader:

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config_data = self._load_config()

    def _load_config(self) -> BenchmarkConfig:
        pass
    
