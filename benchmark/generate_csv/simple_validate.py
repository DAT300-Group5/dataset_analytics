import pandas as pd
import numpy as np
from pathlib import Path
from tabulate import tabulate
import itertools
import warnings
warnings.filterwarnings('ignore')



NUMERIC_RTOL = 1e-5
NUMERIC_ATOL = 1e-8
SAMPLE_ROWS = 10

# --- Timestamp Parsing ---
def try_parse_timestamp(value) -> pd.Timestamp:
    try:
        str_val = str(value).strip()
        if str_val.isdigit():
            num_val = int(str_val)
            if len(str_val) == 13:  # milliseconds
                return pd.to_datetime(num_val, unit='ms').tz_localize(None)
            elif len(str_val) == 10:  # seconds
                return pd.to_datetime(num_val, unit='s').tz_localize(None)
        return pd.to_datetime(str_val)
    except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime):
        return None

def _parse_timestamp_series(series: pd.Series) -> pd.Series:
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
        result.loc[ms_mask[ms_mask].index] = pd.to_datetime(candidates.loc[ms_mask[ms_mask]], unit="ms", errors="coerce", utc=True)
    if s_mask.any():
        result.loc[s_mask[ms_mask].index] = pd.to_datetime(candidates.loc[s_mask[ms_mask]], unit="s", errors="coerce", utc=True)
    remaining_mask = ~(ms_mask | s_mask)
    if remaining_mask.any():
        result.loc[remaining_mask[remaining_mask].index] = pd.to_datetime(candidates.loc[remaining_mask[remaining_mask].index], errors="coerce", utc=True)
    return result

# --- Column Type Inference ---
def _infer_column_type(series1: pd.Series, series2: pd.Series, sample_rows: int = SAMPLE_ROWS) -> str:
    sample = pd.concat([series1.head(sample_rows), series2.head(sample_rows)], ignore_index=True).dropna()
    if sample.empty:
        return "string"
    sample_str = sample.astype("string[python]").str.strip()
    if sample_str.map(lambda v: try_parse_timestamp(v) is not None).all():
        return "timestamp"
    if pd.to_numeric(sample_str, errors="coerce").notna().all():
        return "numeric"
    return "string"

# --- Formatting differences ---
def _format_diff_value(value, max_len: int = 40) -> str:
    if value is None:
        text = "<null>"
    elif isinstance(value, (float, np.floating)) and np.isnan(value):
        text = "<NaN>"
    elif pd.isna(value):
        text = "<NA>"
    else:
        text = str(value)
    return text if len(text) <= max_len else text[: max_len - 1] + "…"

def _print_diff_summary(diff_mask: pd.DataFrame, diff_rows: pd.Series, df1: pd.DataFrame, df2: pd.DataFrame, label1: str, label2: str, max_diff_rows: int = 20, max_columns_per_row: int = 5):
    diff_row_indices = np.flatnonzero(diff_rows.values)
    records = []
    for row_idx in diff_row_indices[:max_diff_rows]:
        diff_columns = np.flatnonzero(diff_mask.iloc[row_idx].values)
        for col_pos in diff_columns[:max_columns_per_row]:
            column_name = diff_mask.columns[col_pos]
            val1 = df1[column_name].iloc[row_idx] if column_name in df1.columns else "<missing>"
            val2 = df2[column_name].iloc[row_idx] if column_name in df2.columns else "<missing>"
            records.append([row_idx + 1, column_name, val1, val2])
    headers = ["row", "column", label1, label2]
    formatted_records = [[row, col, _format_diff_value(v1), _format_diff_value(v2)] for row, col, v1, v2 in records]
    print("❌ Differences detected:")
    print(tabulate(formatted_records, headers=headers, tablefmt="github", stralign="left", numalign="left"))

# --- CSV Comparison ---
def compare_pair(file1: Path, label1: str, file2: Path, label2: str, rtol=NUMERIC_RTOL, atol=NUMERIC_ATOL):
    print(f"\n🔍 Comparing {label1} ↔ {label2}")
    try:
        df1 = pd.read_csv(file1, header=0, low_memory=False)
        df2 = pd.read_csv(file2, header=0, low_memory=False)
    except Exception as e:
        print(f"Error reading CSVs: {e}")
        return True, 1

    if df1.shape != df2.shape:
        print(f"⚠️ Shape mismatch: {df1.shape} vs {df2.shape}")
        return True, 1

    if list(df1.columns) != list(df2.columns):
        print(f"⚠️ Column headers mismatch")
        return True, 0

    one_na = df1.isna() ^ df2.isna()
    both_na = df1.isna() & df2.isna()

    df1_string = df1.astype("string[python]").apply(lambda col: col.str.strip())
    df2_string = df2.astype("string[python]").apply(lambda col: col.str.strip())
    column_types = {col: _infer_column_type(df1[col], df2[col]) for col in df1.columns}
    diff_mask = one_na.copy()

    for col in df1.columns:
        col_type = column_types[col]
        column_diff = pd.Series(False, index=df1.index)
        if col_type == "numeric":
            s1, s2 = pd.to_numeric(df1[col], errors="coerce"), pd.to_numeric(df2[col], errors="coerce")
            valid = (~s1.isna()) & (~s2.isna())
            if valid.any():
                column_diff.loc[valid] = ~np.isclose(s1[valid], s2[valid], rtol=rtol, atol=atol)
            fallback = (~valid) & (~both_na[col])
            if fallback.any():
                column_diff.loc[fallback] = ~(df1_string[col] == df2_string[col])
        elif col_type == "timestamp":
            ts1, ts2 = _parse_timestamp_series(df1_string[col]), _parse_timestamp_series(df2_string[col])
            valid = ts1.notna() & ts2.notna()
            if valid.any():
                column_diff.loc[valid] = ts1[valid] != ts2[valid]
            fallback = (~valid) & (~both_na[col])
            if fallback.any():
                column_diff.loc[fallback] = ~(df1_string[col] == df2_string[col])
        else:
            column_diff = ~(df1_string[col] == df2_string[col]) & (~both_na[col])
        diff_mask[col] |= column_diff

    diff_rows = diff_mask.any(axis=1)
    diff_count = int(diff_rows.sum())
    if diff_count > 0:
        _print_diff_summary(diff_mask, diff_rows, df1, df2, label1, label2)
        print(f"⚠️ Found {diff_count} row(s) with differences")
        return True, diff_count

    print(f"✅ Files are identical ({df1.shape[0]} rows, {df1.shape[1]} columns)")
    return False, 0

# --- Compare multiple CSVs ---
def compare_files(csv_folder: Path):
    csv_files = sorted(csv_folder.glob("*.csv"))
    if len(csv_files) < 2:
        print("Not enough CSV files to compare!")
        return
    result_info = [(f, f.stem) for f in csv_files]
    total_comparisons, failed_comparisons = 0, 0
    for (f1, label1), (f2, label2) in itertools.combinations(result_info, 2):
        has_diff, _ = compare_pair(f1, label1, f2, label2)
        total_comparisons += 1
        if has_diff:
            failed_comparisons += 1
    print(f"\nSummary: {total_comparisons - failed_comparisons} identical, {failed_comparisons} different out of {total_comparisons} comparisons")

# --- Main ---
if __name__ == "__main__":
    # THE PATH TO WHERE EQUIVALENT CSV FILES ARE SAVED
    csv_folder = Path("./output_category") 
    compare_files(csv_folder)
