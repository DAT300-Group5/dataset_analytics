import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import heartpy as hp
from scipy.signal import resample

######################################################################################################################
# Global constants (unchanged)
win_size = 60000 * 5
min_bpm_hz = 0.8
max_bpm_hz = 3.67
new_fs_threshold = 10
######################################################################################################################

HR_VAL = 'HR'

def clean_slices(df: pd.DataFrame):
    """
    Find the first short "per-second" slice (<10 samples) and return its row index.
    Behavior kept the same as original, but with two robustness fixes:
      - Update the reference second once we cross to a new second.
      - If no short slice exists, return 0 (means no trimming), avoiding None downstream.
    """
    seconds_counter = 0
    start_date_second = str(df['fullTime'].iloc[0]).split(':')[-1]

    for index, row in df.iterrows():
        current_date_second = str(row['fullTime']).split(':')[-1]
        if current_date_second == start_date_second:
            seconds_counter += 1
        else:
            if seconds_counter < 10:  # each slice should have 10 datapoints
                return index
            seconds_counter = 0
            # critical fix: after crossing a second, update the reference
            start_date_second = current_date_second

    # If we never found a short slice, return 0 (no cut) to preserve downstream behavior
    return 0

def filter_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reset index, cut from the index returned by clean_slices, and reset index again.
    This preserves original behavior but guarantees a clean RangeIndex afterwards.
    """
    df = df.reset_index(drop=True)
    idx_to_start = clean_slices(df)
    return df.iloc[idx_to_start:].reset_index(drop=True)

### Detect watch - OFF wrist signal
def aggregate_hrm_seconds(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate HRM per 'fullTime' second.
    - Keeps identical output schema and filtering rule (num_off_wrist == 0).
    - Adds minor safety: copy df and fill possible NaNs in num_off_wrist.
    """
    df = df.copy()
    df[HR_VAL] = df[HR_VAL].astype(float)

    dd1 = df.groupby('fullTime').agg({'ts': 'min', HR_VAL: ['mean', 'median']})
    dd1.columns = ['ts', 'HR_mean', 'HR_median']
    dd1 = dd1.reset_index()

    off_flag = (df[HR_VAL] <= 10)
    dd2 = off_flag.groupby(df['fullTime']).sum().reset_index(name='num_off_wrist')

    hrm_aggregated = pd.merge(dd1, dd2, on='fullTime', how='left')
    hrm_aggregated['num_off_wrist'] = hrm_aggregated['num_off_wrist'].fillna(0)

    print("== ground-truth labeling process finished! ==")
    return hrm_aggregated[hrm_aggregated['num_off_wrist'] == 0]

def do_preprocessing(df_ppg_raw: pd.DataFrame, df_hrm_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Keep the same preprocessing pipeline:
    - filter_daily on both PPG and HRM
    - aggregate HRM per second
    - merge and rename 'ts_x' to 'ts'
    """
    df_ppg = filter_daily(df_ppg_raw)
    df_hrm = filter_daily(df_hrm_raw)
    hrm_aggregated = aggregate_hrm_seconds(df_hrm)

    merged_ppg_hrm_filtered = pd.merge(df_ppg, hrm_aggregated, on='fullTime').rename(columns={'ts_x': 'ts'})
    merged_ppg_hrm_filtered.reset_index(inplace=True, drop=True)
    return merged_ppg_hrm_filtered

def ppg_preProcessing_chunk(df_daily: pd.DataFrame, win_size: int):
    """
    Vectorized chunking based on time gaps:
    - A new chunk starts whenever diff(ts) > win_size.
    - Chunk labels start at 0, preserving the original semantics.
    This replaces the while-loop + try/except but keeps identical behavior.
    """
    ts = pd.to_numeric(df_daily['ts'], errors='coerce').fillna(method='ffill').astype(np.int64).to_numpy()
    gap = np.diff(ts, prepend=ts[0])              # first diff = 0 (no gap)
    chunk = np.cumsum(gap > win_size).astype(np.int64)
    df_daily['chunk'] = chunk

def ppg_preProcessing_slicing(df_daily: pd.DataFrame, win_size: int, dict_slice_vol: dict):
    """
    Slice within each chunk using fixed windows of length 'win_size' (in ms).
    - Fills 'slice' and 'index' columns (names & meaning unchanged).
    - Populates dict_slice_vol with slice length per slice id.
    - Prints messages exactly like the original (including the [NaN] case for length < 2).
    Implementation uses structured loops (no exception-driven control flow).
    """
    n = len(df_daily)
    slice_col = np.empty(n, dtype=np.int64)
    index_col = np.arange(n, dtype=np.int64)

    k = 0  # slice id
    i = 0  # global index
    ts = df_daily['ts'].to_numpy()
    chunk = df_daily['chunk'].to_numpy()

    while i < n:
        current_chunk = chunk[i]
        # find end position j of this chunk (inclusive)
        j = i
        while j + 1 < n and chunk[j + 1] == current_chunk:
            j += 1

        # slice inside [i, j] with windows of length win_size
        start = i
        while start <= j:
            start_time = ts[start]
            end_time = start_time + win_size

            r = start
            # advance r within this chunk while ts[r] <= end_time
            while r + 1 <= j and ts[r + 1] <= end_time:
                r += 1

            length = r - start + 1
            if length < 2:
                print("[NaN] data-points # per slice: " + str(length))
            else:
                print("Preprocessing_slicing:", str(current_chunk) + ',' + str(k) + ',' + str(length))

            dict_slice_vol[k] = length
            slice_col[start:r + 1] = k
            k += 1
            start = r + 1  # next slice starts after r

        i = j + 1  # move to the next chunk

    df_daily['slice'] = slice_col
    df_daily['index'] = index_col

    return dict_slice_vol

def extract_sampling_rate(timer):
    """
    Keep only the time part ('%H:%M:%S.%f') and let HeartPy infer the sampling rate.
    Cast elements to str for robustness.
    """
    timer = [str(x).split(' ')[-1] for x in timer]
    fs = hp.get_samplerate_datetime(timer, timeformat='%H:%M:%S.%f')
    return fs

def isolate_HR_frequencies(signal, fs, min_bpm_hz, max_bpm_hz):
    """
    Standard Butterworth bandpass (order=3) to isolate HR frequencies in [min_bpm_hz, max_bpm_hz].
    Behavior identical to original.
    """
    filtered_ = hp.filter_signal(signal, [min_bpm_hz, max_bpm_hz],
                                 sample_rate=fs, order=3, filtertype='bandpass')
    return filtered_

def resample_signal(filtered_signal, fs, fs_range):
    """
    Resample the signal by a factor 'fs_range' and return (resampled, new_fs).
    - target_len uses round() to avoid floating-point off-by-one.
    """
    target_len = int(round(len(filtered_signal) * fs_range))
    resampled = resample(filtered_signal, target_len)
    new_fs = fs * fs_range
    return (resampled, new_fs)

def detect_RR(resampled, new_fs):
    """
    Run HeartPy peak/RR detection with the same configuration as the original code.
    Returns (wd, m).
    """
    wd, m = hp.process(resampled, sample_rate=new_fs, calc_freq=True,
                       high_precision=True, high_precision_fs=1000.0)
    return (wd, m)

def do_feature_extraction(original_df: pd.DataFrame,
                          min_bpm_hz, max_bpm_hz,
                          new_fs_threshold, ppg_slice_no):
    """
    Extract HRV features per slice:
    - Vectorized creation of 'fullTime_ms' from 'ts'.
    - Iterates over slices [0..slice_label] inclusive.
    - Preserves all prints, conditions, column names and values in df_hrv.
    """
    slice_label = int(original_df.tail(1).slice.values[0])
    print("slice label", slice_label)
    df_hrv = pd.DataFrame()
    counter = 0

    # Build 'fullTime_ms' vectorized, then attach (keeps original UTC+9 shift)
    ts_vals = original_df['ts'].to_numpy(dtype=np.int64)
    fulltime_ms = [
        (datetime.utcfromtimestamp(i / 1000).replace(microsecond=(i % 1000) * 1000) + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S.%f")
        for i in ts_vals
    ]
    original_df = original_df.copy()
    original_df['fullTime_ms'] = fulltime_ms

    for i in range(0, slice_label + 1):
        slice_mask = (original_df.slice == i)
        slice_idx = np.flatnonzero(slice_mask.values)
        if slice_idx.size == 0:
            print("[Exception] empty slice:", i)
            continue

        slice_from = int(slice_idx[0])
        slice_to = int(slice_idx[-1])
        print("========")
        print(slice_from)
        print(slice_to)
        print(ppg_slice_no[i])
        print("--------")

        signal_slice = original_df['ppg'].values[slice_from:slice_to]
        timer_slice = original_df['fullTime_ms'].values[slice_from:slice_to]
        ts_start = original_df['ts'].values[slice_from:slice_to]

        if ppg_slice_no[i] > 20:
            # Step 1: Extract sampling rate of the sample
            fs = extract_sampling_rate(timer_slice)
            print("init fs: ", fs)

            '''
            According to the Nyquist–Shannon theorem, when sampling a continuous signal
            the sampling rate should be at least twice that of the highest frequency to be captured
            ==> 3.67*2.
            '''
            if fs > 7.34:
                try:
                    # Step 2: Isolate HR frequencies
                    hr_signal = isolate_HR_frequencies(signal_slice, fs, min_bpm_hz, max_bpm_hz)
                    # Step 3: Upsample to higher frequencies to aid peak detection
                    resampled, new_fs = resample_signal(hr_signal, fs, new_fs_threshold)
                    print('new fs: ', new_fs)

                    upto = int(new_fs * ppg_slice_no[i])
                    print(resampled[0:upto].shape)
                    wd, hrv_analysis = detect_RR(resampled[0:upto], new_fs)
                    print('=====================DETECTED RR========================')
                    observed_ibi = len(wd['RR_list_cor'])

                    mean_hr = original_df['HR_mean'].values[slice_from:slice_to].mean()
                    missingess_ppg = 1 - ((observed_ibi + 1) / float(hrv_analysis['bpm'] * 5))
                    missingess_samsung_bpm = 1 - ((observed_ibi + 1) / float(mean_hr * 5))
                    hr_diff = abs(hrv_analysis['bpm'] - mean_hr)

                    print(slice_from, slice_to)
                    print('ppg bpm:', hrv_analysis['bpm'], ' missingess_ppg: ', missingess_ppg)
                    print('samsung bpm:', mean_hr, ' missingess_samsung_bpm: ', missingess_samsung_bpm)
                    print('hr_diff: ', hr_diff)
                    print("========")

                    df_hrv.at[counter, 'ts_start'] = ts_start[0]
                    df_hrv.at[counter, 'fulltime_start'] = timer_slice[0]
                    df_hrv.at[counter, 'fulltime_end'] = timer_slice[-1]
                    df_hrv.at[counter, 'missingess_ppg'] = missingess_ppg
                    df_hrv.at[counter, 'missingess_samsung_bpm'] = missingess_samsung_bpm
                    df_hrv.at[counter, 'bpm'] = hrv_analysis['bpm']
                    df_hrv.at[counter, 'mean_hr_samsung'] = mean_hr
                    df_hrv.at[counter, 'hr_diff'] = hr_diff
                    df_hrv.at[counter, 'ibi'] = hrv_analysis['ibi']
                    df_hrv.at[counter, 'sdnn'] = hrv_analysis['sdnn']
                    df_hrv.at[counter, 'sdsd'] = hrv_analysis['sdsd']
                    df_hrv.at[counter, 'rmssd'] = hrv_analysis['rmssd']
                    df_hrv.at[counter, 'pnn20'] = hrv_analysis['pnn20']
                    df_hrv.at[counter, 'pnn50'] = hrv_analysis['pnn50']
                    df_hrv.at[counter, 'hr_mad'] = hrv_analysis['hr_mad']
                    df_hrv.at[counter, 'breathingrate'] = hrv_analysis['breathingrate']
                    df_hrv.at[counter, 'lf'] = hrv_analysis['lf']
                    df_hrv.at[counter, 'hf'] = hrv_analysis['hf']
                    df_hrv.at[counter, 'lf/hf'] = hrv_analysis['lf/hf']
                    df_hrv.at[counter, 'observed_ibi'] = observed_ibi

                    counter += 1

                except Exception as e:
                    # Keep the same error print structure: message + time bounds
                    print('error', e, timer_slice[0], timer_slice[-1])
            else:
                print("Nyquist criterion violated:", timer_slice[0], timer_slice[-1], fs)
        else:
            print("[Exception] data-points # per slice: " + str(ppg_slice_no[i]))

    return df_hrv

def do_extractingHRV(df_ppg_filtered: pd.DataFrame):
    """
    Orchestrate preprocessing and feature extraction.
    Behavior identical to original; prints head(), builds chunk & slice, extracts features.
    """
    print(df_ppg_filtered.head())
    dict_slice_vol = {}
    if len(df_ppg_filtered) > 0:
        ppg_preProcessing_chunk(df_ppg_filtered, win_size)
        ppg_slice_no = ppg_preProcessing_slicing(df_ppg_filtered, win_size, dict_slice_vol)

        # extracting the HRV features from the pre-processed ppg signal:
        extracted_features = do_feature_extraction(df_ppg_filtered, min_bpm_hz, max_bpm_hz, new_fs_threshold, ppg_slice_no)
        return extracted_features

'''
rootdir containing 49 directories of 49 participants with the following files:
1) all_days_ppg.csv.gz for PPG signals collected from the user
2) all_days_hrm.csv.gz for HR collected from the user
'''

rootdir = 'data_per_user/'
SAVE_PATH = ''  # specify the path where to save the output of this code

for user in os.listdir(rootdir):
    if '.' in user:
        continue
    try:
        c = pd.read_csv(rootdir + user + '/all_days_ppg.csv.gz').sort_values('ts').reset_index(drop=True)
        c['fullTime'] = [time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime(int(i / 1000))) for i in c.ts]

        c2 = pd.read_csv(rootdir + user + '/all_days_hrm.csv.gz').sort_values('ts').reset_index(drop=True)
        c2['fullTime'] = [time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime(int(i / 1000))) for i in c2.ts]

        data1 = do_preprocessing(c, c2)

        in_data = data1[(data1.ppg >= 0) & (data1.ppg <= 4194304)].reset_index(drop=True)
        data2 = do_extractingHRV(in_data)
        data2.to_csv(SAVE_PATH + '/hrv_computed_' + user + '.csv.gz', index=False)
        print("Completed for user:", user)
    except Exception as e:
        # Preserve behavior of reporting an error, but include the exception text for debugging
        print("error on user:", user, "|", e)