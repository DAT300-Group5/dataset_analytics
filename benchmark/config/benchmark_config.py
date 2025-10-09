"""
Configuration manager for benchmark experiments.

This module provides the BenchmarkConfig class for loading and validating
benchmark configuration from YAML files.
"""

from pathlib import Path
from typing import Any, Dict, List

import yaml

# Constants for configuration validation
SUPPORTED_MODES = {"child", "inproc"}
SUPPORTED_ENGINES = {"duckdb", "sqlite", "chdb"}

ENGINE_DB_MAPPING = {
    "duckdb": "duckdb_db",
    "sqlite": "sqlite_db",
    "chdb": "chdb_db_dir"
}

ENGINE_SQL_MAPPING = {
    "duckdb": "duckdb_sql",
    "sqlite": "sqlite_sql",
    "chdb": "chdb_sql"
}


class BenchmarkConfig:
    """
    Configuration manager for benchmark experiments.
    
    Handles loading and validation of benchmark configuration from YAML file.
    All configuration values are stored as member variables for efficient access.
    
    Attributes:
        config_path (Path): Path to the configuration YAML file
        
        # Core Configuration
        engines (List[str]): List of database engines to benchmark (duckdb, sqlite, chdb)
        mode (str): Execution mode (child, inproc)
        datasets (List[Dict[str, Any]]): Dataset configurations with database paths
        query_groups (List[Dict[str, Any]]): Query group configurations with SQL paths
        
        # Thread Configuration  
        threads_duckdb (int): Number of threads for DuckDB (default: 4)
        threads_chdb (int): Number of threads for ChDB (default: 4)
        threads_sqlite (int): Number of threads for SQLite (default: 0)
        
        # Full Run Configuration
        repeat_full (int): Number of repetitions for full benchmark runs
        warmups_full (int): Number of warmup runs before full benchmarks
        
        # Pilot Run Configuration
        repeat_pilot (int): Number of repetitions for pilot runs
        warmups_pilot (int): Number of warmup runs before pilot runs
        
        # Interval Configuration
        target_samples (int): Target number of samples for interval calculation
        min_interval (float): Minimum interval between benchmark runs (seconds)
        max_interval (float): Maximum interval between benchmark runs (seconds)
    """
    
    def __init__(self, config_path: Path):
        """
        Initialize configuration from YAML file.
        
        Args:
            config_path: Path to configuration YAML file
        """
        self.config_path = config_path
        
        # Load and validate configuration
        config_data = self._load_config()
        self._validate_config(config_data)
        
        # Store all configuration as member variables
        self._parse_and_store_config(config_data)
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate configuration parameters.
        
        Args:
            config: Configuration dictionary to validate
        
        Raises:
            ValueError: If required keys are missing or values are invalid
        """
        required_keys = [
            "datasets", "query_groups", "engines", "mode",
            "repeat_full", "warmups_full", "repeat_pilot", "warmups_pilot",
            "target_samples", "min_interval", "max_interval"
        ]
        
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required configuration key: {key}")
        
        # Validate engines
        unsupported_engines = set(config["engines"]) - SUPPORTED_ENGINES
        if unsupported_engines:
            raise ValueError(f"Unsupported engines: {unsupported_engines}")
        
        # Validate mode
        if config["mode"] not in SUPPORTED_MODES:
            raise ValueError(f"Unsupported mode: {config['mode']}")
    
    def _parse_and_store_config(self, config: Dict[str, Any]) -> None:
        """
        Parse configuration and store as member variables.
        
        Args:
            config: Configuration dictionary from YAML
        """
        # Core configuration
        self.engines: List[str] = config["engines"]
        self.mode: str = config["mode"]
        self.datasets: List[Dict[str, Any]] = config["datasets"]
        self.query_groups: List[Dict[str, Any]] = config["query_groups"]
        
        # Thread configuration per engine
        self.threads_duckdb: int = config.get("threads_duckdb", 4)
        self.threads_chdb: int = config.get("threads_chdb", 4)
        self.threads_sqlite: int = config.get("threads_sqlite", 0)
        
        # Full run configuration
        self.repeat_full: int = config["repeat_full"]
        self.warmups_full: int = config["warmups_full"]
        
        # Pilot run configuration
        self.repeat_pilot: int = config["repeat_pilot"]
        self.warmups_pilot: int = config["warmups_pilot"]
        
        # Interval configuration
        self.target_samples: int = config["target_samples"]
        self.min_interval: float = config["min_interval"]
        self.max_interval: float = config["max_interval"]
    
    def get_threads(self, engine: str) -> int:
        """
        Get thread count for specific engine.
        
        Args:
            engine: Database engine name
        
        Returns:
            Number of threads to use for the engine
        """
        thread_mapping = {
            "duckdb": self.threads_duckdb,
            "chdb": self.threads_chdb,
            "sqlite": self.threads_sqlite,
        }
        return thread_mapping.get(engine, 0)
    
    def get_database_path(self, dataset: Dict[str, Any], engine: str) -> str:
        """
        Get database path for specific dataset and engine.
        
        Args:
            dataset: Dataset configuration dictionary
            engine: Database engine name
        
        Returns:
            Database path
        
        Raises:
            ValueError: If engine is not supported
        """
        if engine not in ENGINE_DB_MAPPING:
            raise ValueError(f"Unsupported engine: {engine}")
        
        db_key = ENGINE_DB_MAPPING[engine]
        if db_key not in dataset:
            raise ValueError(f"Missing database path for engine '{engine}' in dataset '{dataset.get('name', 'unknown')}'")
        
        return dataset[db_key]
    
    def get_sql_path(self, query_group: Dict[str, Any], engine: str) -> str:
        """
        Get SQL path for specific query group and engine.
        
        Args:
            query_group: Query group configuration dictionary
            engine: Database engine name
        
        Returns:
            SQL file path
        
        Raises:
            ValueError: If engine is not supported
        """
        if engine not in ENGINE_SQL_MAPPING:
            raise ValueError(f"Unsupported engine: {engine}")
        
        sql_key = ENGINE_SQL_MAPPING[engine]
        if sql_key not in query_group:
            raise ValueError(f"Missing SQL path for engine '{engine}' in query group '{query_group.get('id', 'unknown')}'")
        
        return query_group[sql_key]
