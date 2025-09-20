#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HRV feature extraction pipeline

Original one: <https://github.com/aitolkyn99/hrv_smartwatch/blob/main/code_heartpy.py>
"""

from __future__ import annotations

import os
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import pandas as pd
import heartpy as hp
from scipy.signal import resample

# ============================================================================ #
#                                Configuration                                 #
# ============================================================================ #

WIN_SIZE_MS: int = 60000 * 5              # 5-minute window in milliseconds
MIN_BPM_HZ: float = 0.8                   # lower cutoff (Hz) for HR band
MAX_BPM_HZ: float = 3.67                  # upper cutoff (Hz) for HR band
NEW_FS_THRESHOLD: int = 10                # upsampling factor
HR_COL: str = "HR"                        # HR column name used across functions

ROOT_DIR: str = "data_per_user/"          # input root directory
SAVE_PATH: str = ""                       # output dir (empty means current dir)

# ============================================================================ #
#                                  Utilities                                   #
# ============================================================================ #


def clean_slices(df: pd.DataFrame) -> int:
    """
    Identify the row index where the first valid 'slice' should begin.

    The function counts consecutive rows that share the same seconds component
    in 'fullTime'. If fewer than 10 points exist for the starting second,
    the returned index marks where to begin after the short run.

    Returns
    -------
    int
        DataFrame index from which a 'valid' sequence starts.
    """
    seconds_counter = 0
    start_second = df["fullTime"].iloc[0].split(":")[-1]

    for index, row in df.iterrows():
        current_second = row["fullTime"].split(":")[-1]
        if current_second == start_second:
            seconds_counter += 1
        else:
            # each slice should have 10 datapoints
            if seconds_counter < 10:
                return index
            seconds_counter = 0

    # If loop completes without hitting the else-branch, start from 0
    return 0


def filter_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reset index and drop early short runs based on seconds grouping.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame starting from the index returned by `clean_slices`.
    """
    df = df.reset_index(drop=True)
    idx_to_start = clean_slices(df)
    return df[idx_to_start::]


def aggregate_hrm_seconds(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate HRM values per second and remove off-wrist segments.

    Off-wrist is defined as HR <= 10 in the original code.
    Keeps seconds where num_off_wrist == 0.
    """
    # Ensure numeric HR
    df[HR_COL] = df[HR_COL].astype(float)

    # Aggregate per fullTime second: min ts, mean HR, median HR
    dd1 = df.groupby("fullTime").agg({"ts": "min", HR_COL: ["mean", "median"]})
    dd1.columns = ["ts", "HR_mean", "HR_median"]
    dd1 = dd1.reset_index()

    # Count off-wrist flags (HR <= 10) per second
    dd2 = (df[HR_COL] <= 10).groupby(df["fullTime"]).sum().reset_index(name="num_off_wrist")

    # Merge and keep only valid seconds
    hrm_aggregated = pd.merge(dd1, dd2, on="fullTime", how="left")
    print("== ground-truth labeling process finished! ==")
    return hrm_aggregated[hrm_aggregated["num_off_wrist"] == 0]


def do_preprocessing(df_ppg_raw: pd.DataFrame, df_hrm_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare PPG and HRM data and merge them on 'fullTime'.

    Returns
    -------
    pd.DataFrame
        PPG joined with aggregated HRM for valid seconds.
    """
    df_ppg = filter_daily(df_ppg_raw)
    df_hrm = filter_daily(df_hrm_raw)
    hrm_aggregated = aggregate_hrm_seconds(df_hrm)

    merged = pd.merge(df_ppg, hrm_aggregated, on="fullTime").rename(columns={"ts_x": "ts"})
    merged = merged.reset_index(drop=True)
    return merged


def ppg_preprocessing_chunk(df_daily: pd.DataFrame, win_size_ms: int) -> None:
    """
    Assign 'chunk' IDs to continuous runs. A new chunk starts when the gap
    between consecutive timestamps exceeds win_size_ms.

    Side Effects
    ------------
    Adds a 'chunk' column to df_daily in place.
    """
    i, j = 0, 0
    chunks: List[int] = []

    while i >= 0:
        try:
            chunks.append(j)
            # start a new chunk if a large gap exists
            if int(df_daily["ts"][i]) < int(df_daily["ts"][i + 1]) - win_size_ms:
                j += 1
            i += 1
        except KeyError:
            break

    df_daily["chunk"] = chunks


def ppg_preprocessing_slicing(
    df_daily: pd.DataFrame, win_size_ms: int, dict_slice_vol: Dict[int, int]
) -> Dict[int, int]:
    """
    Slice each 'chunk' into windows of size win_size_ms and record per-slice counts.

    Returns
    -------
    Dict[int, int]
        Mapping: slice_id -> number_of_points_in_slice

    Side Effects
    ------------
    Adds 'slice' and 'index' columns to df_daily in place.
    """
    i = j = k = 0
    indices: List[int] = []
    slices: List[int] = []

    start_time = df_daily["ts"][i]
    end_time = start_time + win_size_ms

    while i >= 0:
        try:
            indices.append(i)
            slices.append(k)

            same_chunk = df_daily.chunk[i] == df_daily.chunk[i + 1]
            if same_chunk:
                if df_daily["ts"][i] > end_time:
                    dict_slice_vol[k] = i - j
                    start_time = df_daily["ts"][i]
                    end_time = start_time + win_size_ms
                    j = i
                    k += 1
            else:
                if (i + 1 - j) < 2:
                    print("[NaN] data-points # per slice:", str(i + 1 - j))
                else:
                    print("Preprocessing_slicing:", f"{df_daily.chunk[i]},{k},{i + 1 - j}")

                dict_slice_vol[k] = i + 1 - j
                start_time = df_daily["ts"][i + 1]
                end_time = start_time + win_size_ms
                j = i + 1
                k += 1

            i += 1

        except KeyError:
            dict_slice_vol[k] = i + 1 - j
            print("in except", f"{df_daily.chunk[i]},{k},{i + 1 - j}")
            break

    df_daily["slice"] = slices
    df_daily["index"] = indices
    return dict_slice_vol


def extract_sampling_rate(timer: List[str]) -> float:
    """
    Extract sampling rate from a list of timestamp strings using HeartPy.

    The input timer values may include date and time; the function keeps only
    the time portion before calling HeartPy.
    """
    # keep only the time component (HH:MM:SS.microseconds)
    only_time = [x.split(" ")[-1] for x in timer]
    fs = hp.get_samplerate_datetime(only_time, timeformat="%H:%M:%S.%f")
    return fs


def isolate_hr_frequencies(
    signal: List[float], fs: float, min_bpm_hz: float, max_bpm_hz: float
) -> List[float]:
    """
    Apply a Butterworth bandpass filter to keep [min_bpm_hz, max_bpm_hz].
    """
    return hp.filter_signal(
        signal,
        [min_bpm_hz, max_bpm_hz],
        sample_rate=fs,
        order=3,
        filtertype="bandpass",
    )


def resample_signal(filtered_signal: List[float], fs: float, fs_range: int) -> Tuple[List[float], float]:
    """
    Upsample the filtered signal by 'fs_range' using SciPy resample.
    Returns the resampled signal and the new sampling rate.
    """
    resampled_signal = resample(filtered_signal, len(filtered_signal) * fs_range)
    new_fs = fs * fs_range
    return resampled_signal, new_fs


def detect_rr(resampled_signal: List[float], new_fs: float):
    """
    Detect RR intervals using HeartPy with high precision enabled.
    """
    wd, metrics = hp.process(
        resampled_signal,
        sample_rate=new_fs,
        calc_freq=True,
        high_precision=True,
        high_precision_fs=1000.0,
    )
    return wd, metrics


def do_feature_extraction(
    original_df: pd.DataFrame,
    min_bpm_hz: float,
    max_bpm_hz: float,
    new_fs_threshold: int,
    ppg_slice_no: Dict[int, int],
) -> pd.DataFrame:
    """
    Loop over slices, filter within HR band, upsample, detect RR, and collect metrics.

    Returns
    -------
    pd.DataFrame
        A DataFrame of HRV features per slice.
    """
    slice_label = original_df.tail(1).slice.values[0]
    print("slice label", slice_label)

    df_hrv = pd.DataFrame()
    counter = 0

    # Build a high-precision time string (with offset +9 hours as in original)
    original_df["fullTime_ms"] = [
        (
            datetime.fromtimestamp(ts / 1000, tz=timezone.utc) \
            .replace(microsecond=ts % 1000 * 1000) \
            + timedelta(hours=9)
        ).strftime("%Y-%m-%d %H:%M:%S.%f")
        for ts in original_df["ts"]
    ]

    for i in range(0, int(slice_label) + 1):
        slice_df = original_df.loc[original_df.slice == i]
        slice_from = slice_df["index"].head(1).values[0]
        slice_to = slice_df["index"].tail(1).values[0]

        print("========")
        print(slice_from)
        print(slice_to)
        print(ppg_slice_no[i])
        print("--------")

        signal_slice = original_df["ppg"].values[slice_from:slice_to]
        timer_slice = original_df["fullTime_ms"].values[slice_from:slice_to]
        ts_start = original_df["ts"].values[slice_from:slice_to]

        if ppg_slice_no[i] > 20:
            # Step 1: sampling rate from timer
            fs = extract_sampling_rate(list(timer_slice))
            print("init fs:", fs)

            # Nyquist criterion check: fs >= 2 * f_max (3.67 Hz)
            if fs > 7.34:
                try:
                    # Step 2: bandpass in HR range
                    hr_signal = isolate_hr_frequencies(signal_slice, fs, min_bpm_hz, max_bpm_hz)

                    # Step 3: upsample for better peak detection
                    resampled_sig, new_fs = resample_signal(hr_signal, fs, new_fs_threshold)
                    print("new fs:", new_fs)

                    # Truncate to expected length based on new_fs and #points
                    truncated = resampled_sig[0 : int(new_fs * ppg_slice_no[i])]
                    print(truncated.shape)

                    wd, hrv = detect_rr(truncated, new_fs)
                    print("=====================DETECTED RR========================")

                    observed_ibi = len(wd["RR_list_cor"])
                    mean_hr = original_df["HR_mean"].values[slice_from:slice_to].mean()

                    # Missingness metrics (as in original)
                    missingness_ppg = 1 - ((observed_ibi + 1) / float(hrv["bpm"] * 5))
                    missingness_samsung_bpm = 1 - ((observed_ibi + 1) / float(mean_hr * 5))
                    hr_diff = abs(hrv["bpm"] - mean_hr)

                    print(slice_from, slice_to)
                    print("ppg bpm:", hrv["bpm"], " missingess_ppg:", missingness_ppg)
                    print("samsung bpm:", mean_hr, " missingess_samsung_bpm:", missingness_samsung_bpm)
                    print("hr_diff:", hr_diff)
                    print("========")

                    # Collect features
                    df_hrv.at[counter, "ts_start"] = ts_start[0]
                    df_hrv.at[counter, "fulltime_start"] = timer_slice[0]
                    df_hrv.at[counter, "fulltime_end"] = timer_slice[-1]
                    df_hrv.at[counter, "missingess_ppg"] = missingness_ppg
                    df_hrv.at[counter, "missingess_samsung_bpm"] = missingness_samsung_bpm
                    df_hrv.at[counter, "bpm"] = hrv["bpm"]
                    df_hrv.at[counter, "mean_hr_samsung"] = mean_hr
                    df_hrv.at[counter, "hr_diff"] = hr_diff
                    df_hrv.at[counter, "ibi"] = hrv["ibi"]
                    df_hrv.at[counter, "sdnn"] = hrv["sdnn"]
                    df_hrv.at[counter, "sdsd"] = hrv["sdsd"]
                    df_hrv.at[counter, "rmssd"] = hrv["rmssd"]
                    df_hrv.at[counter, "pnn20"] = hrv["pnn20"]
                    df_hrv.at[counter, "pnn50"] = hrv["pnn50"]
                    df_hrv.at[counter, "hr_mad"] = hrv["hr_mad"]
                    df_hrv.at[counter, "breathingrate"] = hrv["breathingrate"]
                    df_hrv.at[counter, "lf"] = hrv["lf"]
                    df_hrv.at[counter, "hf"] = hrv["hf"]
                    df_hrv.at[counter, "lf/hf"] = hrv["lf/hf"]
                    df_hrv.at[counter, "observed_ibi"] = observed_ibi

                    counter += 1

                except Exception as e:
                    # Keep identical behavior: print and continue
                    print("error", e, timer_slice[0], timer_slice[-1])

            else:
                print("Nyquist criterion violated:", timer_slice[0], timer_slice[-1], fs)

        else:
            print("[Exception] data-points # per slice:", str(ppg_slice_no[i]))

    return df_hrv


def do_extracting_hrv(df_ppg_filtered: pd.DataFrame) -> pd.DataFrame | None:
    """
    Entry point for HRV extraction on pre-filtered PPG.

    Returns
    -------
    pd.DataFrame | None
        Extracted feature table, or None when input is empty.
    """
    print(df_ppg_filtered.head())
    dict_slice_vol: Dict[int, int] = {}

    if len(df_ppg_filtered) > 0:
        ppg_preprocessing_chunk(df_ppg_filtered, WIN_SIZE_MS)
        ppg_slice_no = ppg_preprocessing_slicing(df_ppg_filtered, WIN_SIZE_MS, dict_slice_vol)

        # Extract HRV features from the pre-processed PPG signal
        extracted = do_feature_extraction(
            df_ppg_filtered, MIN_BPM_HZ, MAX_BPM_HZ, NEW_FS_THRESHOLD, ppg_slice_no
        )
        return extracted

    return None


# ============================================================================ #
#                                   Main Flow                                  #
# ============================================================================ #


def main() -> None:
    """
    Iterate over user folders, read input CSVs, preprocess, extract HRV,
    and write per-user results to CSV (gz).
    """
    for user in os.listdir(ROOT_DIR):
        # Skip file-like entries
        if "." in user:
            continue

        try:
            # Read and sort PPG
            df_ppg = pd.read_csv(os.path.join(ROOT_DIR, user, "all_days_ppg.csv.gz")).sort_values("ts").reset_index(drop=True)
            df_ppg["fullTime"] = [
                time.strftime("%Y-%m-%d  %H:%M:%S", time.localtime(int(ts / 1000)))
                for ts in df_ppg.ts
            ]

            # Read and sort HRM
            df_hrm = pd.read_csv(os.path.join(ROOT_DIR, user, "all_days_hrm.csv.gz")).sort_values("ts").reset_index(drop=True)
            df_hrm["fullTime"] = [
                time.strftime("%Y-%m-%d  %H:%M:%S", time.localtime(int(ts / 1000)))
                for ts in df_hrm.ts
            ]

            # Preprocessing and merge
            merged = do_preprocessing(df_ppg, df_hrm)

            # Keep PPG within original valid range and extract HRV
            in_data = merged[(merged.ppg >= 0) & (merged.ppg <= 4194304)].reset_index(drop=True)
            features = do_extracting_hrv(in_data)

            # Save per-user results
            if features is not None:
                out_path = os.path.join(SAVE_PATH, f"hrv_computed_{user}.csv.gz")
                features.to_csv(out_path, index=False)
            print("Completed for user:", user)

        except Exception:
            # Keep the same broad except as original
            print("error on user:", user)


if __name__ == "__main__":
    main()
