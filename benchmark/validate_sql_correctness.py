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
from tabulate import tabulate

logger = setup_logger(__name__)


def build_experiment(params: ExperimentParams):
    sql_file = str(params.sql_file.resolve())
    db_file = str(params.db_file.resolve())
    engine_cmd = params.engine_cmd
    cwd = str((params.cwd / params.db_name / params.exp_name).resolve())
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

# Number of rows to sample when inferring column types
SAMPLE_ROWS = 10


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
                return pd.to_datetime(num_val, unit='ms').tz_localize(None)
            # Second timestamp (10 digits)
            elif len(str_val) == 10:
                return pd.to_datetime(num_val, unit='s').tz_localize(None)
        
        # Try parsing as datetime string
        return pd.to_datetime(str_val)
    except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime):
        return None 


def _parse_timestamp_series(series: pd.Series) -> pd.Series:
    """Vectorized timestamp parsing that mimics try_parse_timestamp semantics."""
    # Ensure string dtype for consistent string accessors
    str_series = series.astype("string[python]").str.strip()
    result = pd.Series(pd.NaT, index=str_series.index, dtype="datetime64[ns, UTC]")

    not_na = str_series.notna()
    if not not_na.any():
        return result

    candidates = str_series[not_na]
    digits_mask = candidates.str.fullmatch(r"\d+")
    ms_mask = digits_mask & (candidates.str.len() == 13)
    s_mask = digits_mask & (candidates.str.len() == 10)

    if ms_mask.any():
        ms_index = ms_mask[ms_mask].index
        # Cast candidate strings to numeric before passing to to_datetime with a unit
        # to avoid FutureWarning: parsing strings with 'unit' is deprecated.
        numeric_ms = pd.to_numeric(candidates.loc[ms_index], errors="coerce")
        result.loc[ms_index] = pd.to_datetime(
            numeric_ms, unit="ms", errors="coerce", utc=True
        )
    if s_mask.any():
        s_index = s_mask[s_mask].index
        # Cast candidate strings to numeric before passing to to_datetime with a unit
        numeric_s = pd.to_numeric(candidates.loc[s_index], errors="coerce")
        result.loc[s_index] = pd.to_datetime(
            numeric_s, unit="s", errors="coerce", utc=True
        )

    processed_mask = ms_mask | s_mask
    remaining_candidates = candidates[~processed_mask]
    if not remaining_candidates.empty:
        remaining_index = remaining_candidates.index
        result.loc[remaining_index] = pd.to_datetime(
            remaining_candidates, errors="coerce", utc=True
        )

    return result


def _infer_column_type(series1: pd.Series, series2: pd.Series, sample_rows: int = SAMPLE_ROWS) -> str:
    """Infer column comparison strategy based on the first few rows of both series."""
    sample = pd.concat([series1.head(sample_rows), series2.head(sample_rows)], ignore_index=True)
    sample = sample.dropna()
    if sample.empty:
        return "string"

    sample_str = sample.astype("string[python]").str.strip()

    # Check timestamp first (before numeric), since Unix timestamps are also numeric
    timestamp_candidate = sample_str.map(lambda v: try_parse_timestamp(v) is not None)
    if timestamp_candidate.all():
        return "timestamp"

    numeric_candidate = pd.to_numeric(sample_str, errors="coerce")
    if not numeric_candidate.isna().any():
        return "numeric"

    return "string"


def _format_diff_value(value, max_len: int = 40) -> str:
    """Format a value for diff output, handling NaN/None and truncation."""
    if value is None:
        text = "<null>"
    elif isinstance(value, (float, np.floating)) and np.isnan(value):
        text = "<NaN>"
    elif pd.isna(value):
        text = "<NA>"
    else:
        text = str(value)
    return text if len(text) <= max_len else text[: max_len - 1] + "…"


def _print_diff_summary(
    diff_mask: pd.DataFrame,
    diff_rows: pd.Series,
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    label1: str,
    label2: str,
    max_diff_rows: int = 20,
    max_columns_per_row: int = 5,
) -> None:
    """Print a concise table of differing rows/columns."""
    diff_row_indices = np.flatnonzero(diff_rows.values)

    truncated_columns = False
    records = []

    for row_idx in diff_row_indices[:max_diff_rows]:
        diff_columns = np.flatnonzero(diff_mask.iloc[row_idx].values)
        truncated_columns |= len(diff_columns) > max_columns_per_row
        for col_pos in diff_columns[:max_columns_per_row]:
            column_name = diff_mask.columns[col_pos]
            val1 = df1[column_name].iloc[row_idx] if column_name in df1.columns else "<missing>"
            val2 = df2[column_name].iloc[row_idx] if column_name in df2.columns else "<missing>"
            records.append([row_idx + 1, column_name, val1, val2,
                ]
            )

    headers = ["row", "column", label1, label2]
    formatted_records = [
        [row, column, _format_diff_value(val1), _format_diff_value(val2)]
        for row, column, val1, val2 in records
    ]
    print("  ❌ Differences detected:")
    print(tabulate(formatted_records, headers=headers, tablefmt="github", stralign="left", numalign="left"))


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
        # Read CSV files with pandas, using the first row as header
        df1 = pd.read_csv(file1, header=0, low_memory=False)
        df2 = pd.read_csv(file2, header=0, low_memory=False)
    except Exception as e:
        print(f"  ❌ Error reading CSV files: {e}")
        return True, 1

    # Compare shapes
    if df1.shape != df2.shape:
        has_diff = True
        print(f"  ⚠️  Shape mismatch: {label1} is {df1.shape}, {label2} is {df2.shape}")
        if df1.shape[0] != df2.shape[0]:
            print(f"     Row count: {df1.shape[0]} vs {df2.shape[0]}")
        if df1.shape[1] != df2.shape[1]:
            print(f"     Column count: {df1.shape[1]} vs {df2.shape[1]}")
        return has_diff, 1  # Stop further comparison if shapes differ
    
    rows = df1.shape[0]

    header_diff = False
    if list(df1.columns) != list(df2.columns):
        header_diff = True
        print(f"  ⚠️  Column header mismatch")
        print(f"     {label1} columns: {list(df1.columns)}")
        print(f"     {label2} columns: {list(df2.columns)}")
        # Return early if headers differ
        return True, 0

    one_na = df1.isna() ^ df2.isna()
    both_na = df1.isna() & df2.isna()

    df1_string = df1.astype("string[python]").apply(lambda col: col.str.strip())
    df2_string = df2.astype("string[python]").apply(lambda col: col.str.strip())

    column_types = {
        column: _infer_column_type(df1[column], df2[column]) for column in df1.columns
    }

    diff_mask = one_na.copy()

    for column in df1.columns:
        col_type = column_types[column]
        column_diff = pd.Series(False, index=df1.index)

        if col_type == "numeric":
            series1_num = pd.to_numeric(df1[column], errors="coerce")
            series2_num = pd.to_numeric(df2[column], errors="coerce")

            numeric_valid = (~series1_num.isna()) & (~series2_num.isna())
            if numeric_valid.any():
                close_mask = np.isclose(series1_num[numeric_valid], series2_num[numeric_valid], rtol=rtol, atol=atol)
                column_diff.loc[numeric_valid] = ~close_mask

            fallback_mask = (~numeric_valid) & (~both_na[column])
            if fallback_mask.any():
                same_strings = (df1_string[column] == df2_string[column]).fillna(False)
                column_diff.loc[fallback_mask] = ~same_strings.loc[fallback_mask]

        elif col_type == "timestamp":
            ts1 = _parse_timestamp_series(df1_string[column])
            ts2 = _parse_timestamp_series(df2_string[column])

            timestamp_valid = ts1.notna() & ts2.notna()
            if timestamp_valid.any():
                column_diff.loc[timestamp_valid] = ts1.loc[timestamp_valid] != ts2.loc[timestamp_valid]

            fallback_mask = (~timestamp_valid) & (~both_na[column])
            if fallback_mask.any():
                same_strings = (df1_string[column] == df2_string[column]).fillna(False)
                column_diff.loc[fallback_mask] = ~same_strings.loc[fallback_mask]

        else:  # Treat as string comparison
            same_strings = (df1_string[column] == df2_string[column]).fillna(False)
            column_diff = (~same_strings) & (~both_na[column])

        diff_mask[column] = diff_mask[column] | column_diff

    diff_rows = diff_mask.any(axis=1)
    diff_count = int(diff_rows.sum())
    has_diff = (diff_count > 0) or header_diff

    if diff_count > 0:
        _print_diff_summary(diff_mask, diff_rows, df1, df2, label1, label2)
    elif header_diff:
        print("  ❌ Column headers differ")

    if has_diff and diff_count > 0:
        print(f"  ⚠️  Found {diff_count} row(s) with differences")
    elif has_diff:
        print("  ⚠️  Detected differences in column headers")
    else:
        print(f"  ✅ Results are identical ({df1.shape[0]} rows, {df1.shape[1]} columns)")

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

    config_path = Path(__file__).parent / "config_yaml"
    config = ConfigLoader(config_path, env=args.env)
    experiments = config.get_validation_experiments()
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
