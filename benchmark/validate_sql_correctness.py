import difflib
import itertools
from pathlib import Path
from typing import List, Tuple

from config.config_loader import ConfigLoader
from consts.EngineType import EngineType
from consts.RunMode import RunMode
from models.experiment_params import ExperimentParams
from service.runner.duckdb_runner import DuckdbRunner
from service.runner.sqlite_runner import SQLiteRunner


def build_experiment(params: ExperimentParams):
    sql_file = str(params.sql_file.resolve())
    db_file = str(params.db_file.resolve())
    engine_cmd = params.engine_cmd
    cwd = str((params.cwd / params.exp_name).resolve())
    if params.engine == EngineType.SQLITE:
        runner = SQLiteRunner(sql_file=sql_file, db_file=db_file, cmd=engine_cmd, cwd=cwd, run_mode=RunMode.VALIDATE)
        return runner
    elif params.engine == EngineType.DUCKDB:
        runner = DuckdbRunner(sql_file=sql_file, db_file=db_file, cmd=engine_cmd, cwd=cwd, run_mode=RunMode.VALIDATE)
        return runner


def compare_pair(file1: Path, label1: str, file2: Path, label2: str):
    """Compare two files line by line with custom labels."""
    text1 = file1.read_text().splitlines()
    text2 = file2.read_text().splitlines()

    max_len = max(len(text1), len(text2))
    has_diff = False

    print(f"\n🔍 Comparing {label1} ↔ {label2}")

    for i in range(min(max_len,20)):
        line1 = text1[i] if i < len(text1) else "<NO LINE>"
        line2 = text2[i] if i < len(text2) else "<NO LINE>"

        if line1 != line2:
            has_diff = True
            print(f"❌ Line {i + 1}:")
            print(f"   {label1}: {line1}")
            print(f"   {label2}: {line2}")
            print("")

    if not has_diff:
        print(f"✅ {label1} and {label2} are identical.\n")


def compare_files(result_info: List[Tuple[Path, str, str]]):
    """Compare all files pairwise with experiment context.

    Args:
        result_info: List of tuples (file_path, group_id, engine)
    """
    if len(result_info) < 2:
        raise ValueError("Need at least two files to compare.")

    for (f1, g1, e1), (f2, g2, e2) in itertools.combinations(result_info, 2):
        label1 = f"[{g1}_{e1.value}]"
        label2 = f"[{g2}_{e2.value}]"
        compare_pair(f1, label1, f2, label2)


def main():
    config_path = Path(__file__).parent / "config.yaml"
    config = ConfigLoader(config_path)
    experiments = config.get_experiments()
    validate_pairs = config.config_data.validate_pairs
    print(f"Loaded {len(experiments)} experiments from config")
    print(f"Validate pairs: {validate_pairs}")

    result_info = []
    for experiment in experiments:
        if (experiment.group_id, experiment.engine) in validate_pairs:
            print(f"Running validation for {experiment.exp_name}")
            runner = build_experiment(experiment)
            process = runner.run_subprocess()
            process.wait()
            result_file = experiment.cwd / experiment.exp_name / "result.csv"
            result_info.append((result_file, experiment.group_id, experiment.engine))
            print(f"Validation completed for {experiment.exp_name}, check results in {runner.results_dir}")

    print("\nAll validations completed. Comparing results:")
    compare_files(result_info)


if __name__ == "__main__":
    main()