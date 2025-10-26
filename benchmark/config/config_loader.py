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
from config.execution_unit import ExecutionUnit
from config.query_group import QueryGroup
from consts.EngineType import EngineType
from models.experiment_params import ExperimentParams


class ConfigLoader:

    def __init__(self, config_path: Path, env: str = None):
        self.config_path = config_path
        self.env = env
        self.config_data = self._load_config()
        self.experiments = None

    def _load_config(self) -> BenchmarkConfig:
        """
        Load and parse benchmark configuration from YAML file.
        Supports environment-specific overrides via config_<env>.yaml
        
        Returns:
            BenchmarkConfig: Configured benchmark configuration instance
        """
        # Load base YAML file
        base_config_file = self.config_path / "config.yaml"
        with open(base_config_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        
        # Load environment-specific override if specified
        if self.env:
            env_config_file = self.config_path / f"config_{self.env}.yaml"
            with open(env_config_file, "r", encoding="utf-8") as f:
                env_data = yaml.safe_load(f)
                # Merge environment config into base config
                # dict.update() will overwrite existing keys
                data.update(env_data)
        
        # Create BenchmarkConfig instance
        config = BenchmarkConfig()
        
        # Parse engines
        config.engines = [EngineType(engine) for engine in data["engines"]]

        if "chdb_library_path" in data:
            config.chdb_library_path = data["chdb_library_path"]
        else:
            config.chdb_library_path = ""

        # Parse simple fields
        config.repeat_pilot = data["repeat_pilot"]
        config.sample_count = data["sample_count"]
        config.std_repeat = data["std_repeat"]
        config.cwd = data["output_cwd"]
        config.compare_pairs = []
        config.validate_pairs = []

        config.execute_pairs = [ExecutionUnit(group_id, EngineType(engine)) for group_id, engine in data["execute_pairs"]]

        if "compare_pairs" in data:
            config.compare_pairs = [ExecutionUnit(group_id, EngineType(engine)) for group_id, engine in data["compare_pairs"]]

        if "validate_pairs" in data:
            config.validate_pairs = [ExecutionUnit(group_id, EngineType(engine)) for group_id, engine in data["validate_pairs"]]

        # Parse engine paths
        config.engine_paths = data.get("engine_paths", {})
        
        # Parse datasets
        config.datasets = [Dataset(**ds) for ds in data["datasets"]]
        
        # Parse query groups
        config.query_groups = [QueryGroup(**qg) for qg in data["query_groups"]]
        
        return config

    def filter_experiments(self, execution_units: List[ExecutionUnit], include_ban_ops : bool) -> List[ExperimentParams]:
        """
        Filter experiments based on provided execution units.

        Args:
            execution_units (List[ExecutionUnit]): List of execution units to filter by

        Returns:
            List[ExperimentParams]: Filtered list of experiment parameters
        """
        all_experiments = self.get_experiments()
        filtered_experiments = []
        for exp in all_experiments:
            for item in execution_units:
                if exp.group_id == item.group_id and exp.engine == item.engine and (include_ban_ops or not exp.ban_optimizer):
                    filtered_experiments.append(exp)
                    break
        return filtered_experiments

    def get_validation_experiments(self) -> List[ExperimentParams]:
        first_db = self.config_data.datasets[0]
        experiments = self.filter_experiments(self.config_data.validate_pairs, include_ban_ops=False)
        return [exp for exp in experiments if exp.db_name == first_db.name]

    def get_experiments(self) -> List[ExperimentParams]:
        """
        Generate a list of ExperimentParams for all combinations of datasets,
        query groups, and engines.

        Returns:
            List[ExperimentParams]: List of experiment parameters
        """
        if self.experiments is not None:
            return self.experiments

        experiments = []

        for dataset in self.config_data.datasets:
            for query_group in self.config_data.query_groups:
                for engine in self.config_data.engines:
                    sql_file = getattr(query_group, f"{engine.value}_sql")
                    db_file = getattr(dataset, f"{engine.value}_db", None) or getattr(dataset, f"{engine.value}_db_dir")

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
                            db_name=dataset.name,
                            group_id=query_group.id,
                            ban_optimizer=False,
                            engine_cmd=engine_cmd,
                            chdb_library_path=self.config_data.chdb_library_path,
                            cwd=Path(self.config_data.cwd),
                            sample_count=self.config_data.sample_count,
                            std_repeat=self.config_data.std_repeat
                        )
                        experiments.append(exp_params)
                        # print(f"Created experiment", exp_params)

                    sql_ban_optimizer = getattr(query_group, f"{engine.value}_sql_ban_ops", None)
                    if sql_ban_optimizer and db_file:
                        exp_name = f"{query_group.id}_{engine.value}_ban_ops"

                        engine_cmd = self.config_data.engine_paths.get(engine.value, engine.value)

                        exp_params = ExperimentParams(
                            engine=engine,
                            sql_file=Path(sql_ban_optimizer),
                            db_file=Path(db_file),
                            db_name=dataset.name,
                            exp_name=exp_name,
                            group_id=query_group.id,
                            ban_optimizer=True,
                            engine_cmd=engine_cmd,
                            chdb_library_path=self.config_data.chdb_library_path,
                            cwd=Path(self.config_data.cwd),
                            sample_count=self.config_data.sample_count,
                            std_repeat=self.config_data.std_repeat
                        )
                        experiments.append(exp_params)
                        # print(f"Created experiment with banned optimizer", exp_params)

        self.experiments = experiments
        return experiments
    
if __name__ == "__main__":
    
    # python3 -m config.config_loader
    
    from util.file_utils import project_root
    
    root = project_root()
    config = ConfigLoader(root / "benchmark/config_yaml", env="dev")
    
    config.get_experiments()
    print(config.config_data)
