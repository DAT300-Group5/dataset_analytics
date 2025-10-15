import difflib
import itertools
import sys
from pathlib import Path
from typing import List, Tuple

from config.config_loader import ConfigLoader
from consts.EngineType import EngineType
from consts.RunMode import RunMode
from models.experiment_params import ExperimentParams
from service.runner.duckdb_runner import DuckdbRunner
from service.runner.sqlite_runner import SQLiteRunner
from util.log_config import setup_logger


logger = setup_logger(__name__)


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


def compare_pair(file1: Path, label1: str, file2: Path, label2: str) -> Tuple[bool, int]:
    """Compare two files line by line with custom labels.
    
    Returns:
        Tuple of (has_diff, diff_count)
    """
    text1 = file1.read_text().splitlines()
    text2 = file2.read_text().splitlines()

    max_len = max(len(text1), len(text2))
    has_diff = False
    diff_count = 0

    print(f"\n🔍 {label1} ↔ {label2}")

    for i in range(min(max_len, 20)):
        line1 = text1[i] if i < len(text1) else "<NO LINE>"
        line2 = text2[i] if i < len(text2) else "<NO LINE>"

        if line1 != line2:
            has_diff = True
            diff_count += 1
            print(f"  ❌ Line {i + 1}:")
            print(f"     {label1}: {line1}")
            print(f"     {label2}: {line2}")

    if not has_diff:
        print(f"  ✅ Results are identical")
    else:
        print(f"  ⚠️  Found {diff_count} difference(s)")
    
    return has_diff, diff_count


def compare_files(result_info: List[Tuple[Path, str, str]]) -> Tuple[int, int]:
    """Compare all files pairwise with experiment context.

    Args:
        result_info: List of tuples (file_path, group_id, engine)
    
    Returns:
        Tuple of (total_comparisons, failed_comparisons)
    """
    if len(result_info) < 2:
        raise ValueError("Need at least two files to compare.")

    total_comparisons = 0
    failed_comparisons = 0

    for (f1, g1, e1), (f2, g2, e2) in itertools.combinations(result_info, 2):
        label1 = f"{g1}_{e1.value}"
        label2 = f"{g2}_{e2.value}"
        has_diff, _ = compare_pair(f1, label1, f2, label2)
        total_comparisons += 1
        if has_diff:
            failed_comparisons += 1
    
    return total_comparisons, failed_comparisons


def main():
    print("\n" + "=" * 60)
    print("  SQL CORRECTNESS VALIDATION")
    print("=" * 60)

    config_path = Path(__file__).parent / "config.yaml"
    config = ConfigLoader(config_path)
    experiments = config.get_experiments()
    validate_pairs = config.config_data.validate_pairs
    
    print(f"\n📋 Configuration:")
    print(f"   • Total experiments: {len(experiments)}")
    print(f"   • Validation pairs: {len(validate_pairs)}")

    print(f"\n🔧 Running validations...")
    result_info = []
    for idx, experiment in enumerate(experiments, 1):
        if (experiment.group_id, experiment.engine) in validate_pairs:
            print(f"   [{idx}] {experiment.exp_name}...", end=" ", flush=True)
            runner = build_experiment(experiment)
            process = runner.run_subprocess()
            process.wait()
            stderr = (runner.results_dir / "stderr.log").read_text()
            if process.returncode != 0 or stderr:
                print("❌")
                print(f"\n{'=' * 60}")
                print(f"  ERROR: Validation failed for {experiment.exp_name}")
                print("=" * 60)
                print(f"   Return code: {process.returncode}")
                if stderr:
                    print(f"\n   Error output:")
                    print(f"   {stderr.strip()}")
                print("\n" + "=" * 60)
                print("   Validation aborted due to execution failure.")
                print("=" * 60 + "\n")
                sys.exit(1)
            result_file = experiment.cwd / experiment.exp_name / "result.csv"
            result_info.append((result_file, experiment.group_id, experiment.engine))
            print("✓")

    print(f"\n{'=' * 60}")
    print("  RESULTS COMPARISON")
    print("=" * 60)
    
    total_comparisons, failed_comparisons = compare_files(result_info)

    print(f"\n{'=' * 60}")
    print("  SUMMARY")
    print("=" * 60)
    print(f"   • Total comparisons: {total_comparisons}")
    print(f"   • Identical: {total_comparisons - failed_comparisons}")
    print(f"   • Different: {failed_comparisons}")
    
    if failed_comparisons == 0:
        print(f"\n   ✅ All validations passed!")
    else:
        print(f"\n   ⚠️  {failed_comparisons} comparison(s) failed!")
    
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()