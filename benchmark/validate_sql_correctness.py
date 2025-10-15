import difflib
import itertools
from pathlib import Path
from typing import List

from config.config_loader import ConfigLoader
from consts.EngineType import EngineType
from consts.RunMode import RunMode
from models.experiment_params import ExperimentParams
from service.runner.duckdb_runner import DuckdbRunner
from service.runner.sqlite_runner import SQLiteRunner


def build_experiment(params : ExperimentParams) :
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


def compare_files(file_list: List[Path]):
    """Compare all files pairwise and print line-by-line differences."""
    if len(file_list) < 2:
        raise ValueError("Need at least two files to compare.")

    for f1, f2 in itertools.combinations(file_list, 2):
        text1 = f1.read_text().splitlines()
        text2 = f2.read_text().splitlines()

        max_len = max(len(text1), len(text2))
        has_diff = False

        print(f"\n🔍 Comparing {f1.name} ↔ {f2.name}")

        for i in range(max_len):
            line1 = text1[i] if i < len(text1) else "<NO LINE>"
            line2 = text2[i] if i < len(text2) else "<NO LINE>"

            if line1 != line2:
                has_diff = True
                print(f"❌ Line {i+1}:")
                print(f"   {f1.name}: {line1}")
                print(f"   {f2.name}: {line2}")
                print("")

        if not has_diff:
            print(f"✅ {f1.resolve()} and {f2.resolve()} are identical.\n")


def main():
    config_path = Path(__file__).parent / "config.yaml"
    config = ConfigLoader(config_path)
    experiments = config.get_experiments()
    validate_pairs = config.config_data.validate_pairs
    print(f"Loaded {len(experiments)} experiments from config")
    print(f"Validate pairs: {validate_pairs}")
    results = []
    for experiment in experiments:
        if (experiment.group_id, experiment.engine) in validate_pairs:
            print(f"Running validation for {experiment.exp_name}")
            runner = build_experiment(experiment)
            process = runner.run_subprocess()
            process.wait()
            results.append(experiment.cwd / experiment.exp_name / "result.csv")
            print(f"Validation completed for {experiment.exp_name}, check results in {runner.results_dir}")
    print("All validations completed. Results directories:")
    compare_files(results)


if __name__ == "__main__":
    main()