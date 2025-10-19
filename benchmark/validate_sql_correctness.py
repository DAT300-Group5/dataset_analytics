import itertools
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import numpy as np

from config.config_loader import ConfigLoader
from consts.EngineType import EngineType
from consts.RunMode import RunMode
from models.experiment_params import ExperimentParams
from service.runner.duckdb_runner import DuckdbRunner
from service.runner.sqlite_runner import SQLiteRunner
from service.runner.chdb_runner import ChdbRunner
from cli.cli import parse_env_args
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
    elif params.engine == EngineType.CHDB:
        runner = ChdbRunner(sql_file=sql_file, db_file=db_file, cmd=engine_cmd, cwd=cwd, run_mode=RunMode.VALIDATE)
        runner.set_library_path(params.chdb_library_path)
        return runner
    else:
        raise ValueError(f"Unsupported engine for validation: {params.engine}")   


# Numeric comparison tolerance
# rtol: relative tolerance (1e-5 means 0.001% difference is acceptable)
# atol: absolute tolerance (1e-8 for very small numbers)
NUMERIC_RTOL = 1e-5
NUMERIC_ATOL = 1e-8


def try_parse_timestamp(value) -> pd.Timestamp:
    """
    Try to parse a value as timestamp (Unix timestamp or datetime string).
    
    Supports:
    - Unix timestamp in seconds (10 digits)
    - Unix timestamp in milliseconds (13 digits)
    - ISO 8601 datetime strings
    - Common datetime formats
    
    Returns:
        pd.Timestamp if successful, None otherwise
    """
    try:
        str_val = str(value).strip()
        
        # Check if it's a numeric timestamp
        if str_val.isdigit():
            num_val = int(str_val)
            # Millisecond timestamp (13 digits)
            if len(str_val) == 13:
                return pd.to_datetime(num_val, unit='ms')
            # Second timestamp (10 digits)
            elif len(str_val) == 10:
                return pd.to_datetime(num_val, unit='s')
        
        # Try parsing as datetime string
        return pd.to_datetime(str_val)
    except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime):
        return None 


def compare_pair(file1: Path, label1: str, file2: Path, label2: str, rtol=1e-5, atol=1e-8) -> Tuple[bool, int]:
    """Compare two CSV files using pandas with numeric tolerance.
    
    Args:
        file1: First CSV file path
        label1: Label for first file
        file2: Second CSV file path
        label2: Label for second file
        rtol: Relative tolerance for numeric comparison (default: 1e-5)
        atol: Absolute tolerance for numeric comparison (default: 1e-8)
    
    Returns:
        Tuple of (has_diff, diff_count)
    """
    print(f"\n🔍 {label1} ↔ {label2}")
    
    try:
        # Read CSV files with pandas
        df1 = pd.read_csv(file1, header=None)
        df2 = pd.read_csv(file2, header=None)
    except Exception as e:
        print(f"  ❌ Error reading CSV files: {e}")
        return True, 1
    
    has_diff = False
    diff_count = 0
    
    # Compare shapes
    if df1.shape != df2.shape:
        has_diff = True
        print(f"  ⚠️  Shape mismatch: {label1} is {df1.shape}, {label2} is {df2.shape}")
        if df1.shape[0] != df2.shape[0]:
            print(f"     Row count: {df1.shape[0]} vs {df2.shape[0]}")
        if df1.shape[1] != df2.shape[1]:
            print(f"     Column count: {df1.shape[1]} vs {df2.shape[1]}")
    
    # Compare data (only for overlapping rows/columns)
    min_rows = min(df1.shape[0], df2.shape[0])
    min_cols = min(df1.shape[1], df2.shape[1])
    
    # Limit output to first 20 rows
    display_limit = min(min_rows, 20)
    
    for row_idx in range(display_limit):
        diff_cols = []
        
        for col_idx in range(min_cols):
            val1 = df1.iloc[row_idx, col_idx]
            val2 = df2.iloc[row_idx, col_idx]
            
            # Check if values are different
            values_differ = False
            
            # Both are numeric
            if pd.api.types.is_numeric_dtype(type(val1)) and pd.api.types.is_numeric_dtype(type(val2)):
                # Handle NaN
                if pd.isna(val1) and pd.isna(val2):
                    continue
                elif pd.isna(val1) or pd.isna(val2):
                    values_differ = True
                else:
                    # Use numpy's isclose for numeric comparison with tolerance
                    if not np.isclose(val1, val2, rtol=rtol, atol=atol):
                        values_differ = True
            else:
                # String comparison (convert to string for consistency)
                str1 = str(val1).strip()
                str2 = str(val2).strip()
                
                # Try timestamp comparison first
                ts1 = try_parse_timestamp(str1)
                ts2 = try_parse_timestamp(str2)
                
                if ts1 is not None and ts2 is not None:
                    # Both are valid timestamps, compare them
                    if ts1 != ts2:
                        values_differ = True
                else:
                    # Try numeric comparison if both can be converted to float
                    try:
                        num1 = float(str1)
                        num2 = float(str2)
                        if not np.isclose(num1, num2, rtol=rtol, atol=atol):
                            values_differ = True
                    except (ValueError, TypeError):
                        # Fall back to string comparison
                        if str1 != str2:
                            values_differ = True
            
            if values_differ:
                diff_cols.append((col_idx, val1, val2))
        
        if diff_cols:
            has_diff = True
            diff_count += 1
            print(f"  ❌ Row {row_idx + 1}: {len(diff_cols)} column(s) differ")
            for col_idx, val1, val2 in diff_cols[:5]:  # Limit to 5 columns per row
                print(f"     Column {col_idx}: '{val1}' ≠ '{val2}'")
            if len(diff_cols) > 5:
                print(f"     ... and {len(diff_cols) - 5} more column(s)")
    
    if min_rows > 20:
        print(f"  ℹ️  (Showing first 20 of {min_rows} rows)")
    
    if not has_diff:
        print(f"  ✅ Results are identical ({df1.shape[0]} rows, {df1.shape[1]} columns)")
    else:
        print(f"  ⚠️  Found {diff_count} row(s) with differences")
    
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
        has_diff, _ = compare_pair(f1, label1, f2, label2, rtol=NUMERIC_RTOL, atol=NUMERIC_ATOL)
        total_comparisons += 1
        if has_diff:
            failed_comparisons += 1
    
    return total_comparisons, failed_comparisons


def main():
    # Parse command line arguments
    args = parse_env_args("Validate SQL correctness across database engines")
    print("\n" + "=" * 60)
    print("  SQL CORRECTNESS VALIDATION")
    print("=" * 60)

    config_path = Path(__file__).parent
    config = ConfigLoader(config_path, env=args.env)
    experiments = config.filter_experiments(config.config_data.validate_pairs)
    validate_pairs = [(experiment.group_id, experiment.engine) for experiment in config.config_data.validate_pairs]

    print(f"\n📋 Configuration:")
    print(f"   • Total experiments: {len(experiments)}")
    print(f"   • Validation pairs: {len(validate_pairs)}")
    print(f"   • Numeric tolerance: rtol={NUMERIC_RTOL}, atol={NUMERIC_ATOL}")
    print(f"   • Timestamp auto-conversion: enabled")

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
            result_file = runner.results_dir / "result.csv"
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