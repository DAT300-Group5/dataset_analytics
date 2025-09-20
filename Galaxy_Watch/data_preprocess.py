#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Preprocess mixed PPG/HRM datasets (18GB gzipped CSVs) by splitting into
per-device files under data_per_user/<deviceId>/.

Input:
  - ppg.csv.gz with columns: deviceId, ts, ppg
  - hrm.csv.gz with columns: deviceId, ts, HR

Output (one folder per deviceId):
  data_per_user/<deviceId>/all_days_ppg.csv.gz
  data_per_user/<deviceId>/all_days_hrm.csv.gz

Notes:
  - Streaming chunked processing (low memory).
  - No global sorting per user; downstream can sort on read if needed.
  - Uses multi-member gzip appending (common readers handle it fine).
"""

import os
import gzip
import argparse
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import numpy as np


# ----------------------------- Config ----------------------------- #

DEFAULT_OUT_ROOT = "data_per_user"
# Tune this depending on memory and IO throughput. 500k–2M is typical.
DEFAULT_CHUNKSIZE = 1_000_000
# If True, filter obviously invalid values early to reduce size
ENABLE_BASIC_SANITY_FILTERS = True


# --------------------------- Utilities ---------------------------- #

def ensure_dir(p: Path) -> None:
    """Create directory if it does not exist."""
    p.mkdir(parents=True, exist_ok=True)


def open_gz_append(path: Path):
    """
    Open a gzip file for appending.
    This produces a multi-member gzip stream (commonly supported by readers).
    """
    # mode "ab" creates/extends the file in binary append
    return gzip.open(path, mode="ab")


def write_chunk_to_gz(
    df: pd.DataFrame,
    out_path: Path,
    header_written_cache: Dict[Path, bool],
) -> None:
    """
    Append a DataFrame chunk to a gzipped CSV.
    Keeps track of whether we've written a header for this path.
    """
    ensure_dir(out_path.parent)
    # Determine if header should be written
    write_header = not header_written_cache.get(out_path, False) and not out_path.exists()

    # Use a temporary in-memory CSV then gzip-append the bytes (fast and simple)
    csv_bytes = df.to_csv(index=False, header=write_header).encode("utf-8")
    with open_gz_append(out_path) as f:
        f.write(csv_bytes)

    header_written_cache[out_path] = True


def basic_sanity_filters_ppg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optional: very lightweight filters to remove obvious garbage rows early.
    """
    # Coerce types safely
    df["ts"] = pd.to_numeric(df["ts"], errors="coerce")
    df["ppg"] = pd.to_numeric(df["ppg"], errors="coerce")

    # Drop rows with missing critical fields
    df = df.dropna(subset=["deviceId", "ts", "ppg"])

    # Remove negative timestamps and out-of-range PPG values
    df = df[(df["ts"] >= 0)]
    # Range observed in your downstream script; keep consistent
    df = df[(df["ppg"] >= 0) & (df["ppg"] <= 4_194_304)]
    return df


def basic_sanity_filters_hrm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optional: very lightweight filters to remove obvious garbage rows early.
    """
    df["ts"] = pd.to_numeric(df["ts"], errors="coerce")
    df["HR"] = pd.to_numeric(df["HR"], errors="coerce")
    df = df.dropna(subset=["deviceId", "ts", "HR"])
    df = df[(df["ts"] >= 0)]
    # Keep all HR values; downstream handles off-wrist (e.g., HR <= 10) later
    return df


# ------------------------- Core processing ------------------------ #

def process_ppg_file(
    ppg_path: Path,
    out_root: Path,
    chunksize: int = DEFAULT_CHUNKSIZE,
    enable_filters: bool = ENABLE_BASIC_SANITY_FILTERS,
    only_devices: Optional[set] = None,
    summary_counter: Optional[Dict[str, int]] = None,
) -> None:
    """
    Stream the mixed PPG file and split rows per deviceId into:
      data_per_user/<deviceId>/all_days_ppg.csv.gz
    """
    usecols = ["deviceId", "ts", "ppg"]
    dtype_map = {
        "deviceId": "string",
        # ts/ppg coerced later; specifying here as objects avoids parser errors
        "ts": "object",
        "ppg": "object",
    }

    header_written: Dict[Path, bool] = {}

    for chunk_idx, df in enumerate(pd.read_csv(ppg_path, usecols=usecols, dtype=dtype_map,
                                               chunksize=chunksize, compression="gzip")):
        if enable_filters:
            df = basic_sanity_filters_ppg(df)
        else:
            df = df.dropna(subset=["deviceId", "ts", "ppg"])

        if df.empty:
            continue

        # Optionally restrict to a subset of device IDs
        if only_devices is not None:
            df = df[df["deviceId"].isin(only_devices)]
            if df.empty:
                continue

        # Group per device within the chunk and append to respective files
        for dev, g in df.groupby("deviceId", sort=False):
            dev_str = str(dev)
            out_dir = out_root / dev_str
            out_fp = out_dir / "all_days_ppg.csv.gz"

            # Keep original column ordering
            g = g[["deviceId", "ts", "ppg"]]

            write_chunk_to_gz(g, out_fp, header_written)

            if summary_counter is not None:
                summary_counter[dev_str] = summary_counter.get(dev_str, 0) + len(g)

        if (chunk_idx + 1) % 10 == 0:
            print(f"[PPG] Processed {chunk_idx + 1} chunks...")

    print("[PPG] Finished splitting by device.")


def process_hrm_file(
    hrm_path: Path,
    out_root: Path,
    chunksize: int = DEFAULT_CHUNKSIZE,
    enable_filters: bool = ENABLE_BASIC_SANITY_FILTERS,
    only_devices: Optional[set] = None,
    summary_counter: Optional[Dict[str, int]] = None,
) -> None:
    """
    Stream the mixed HRM file and split rows per deviceId into:
      data_per_user/<deviceId>/all_days_hrm.csv.gz
    """
    usecols = ["deviceId", "ts", "HR"]
    dtype_map = {
        "deviceId": "string",
        "ts": "object",
        "HR": "object",
    }

    header_written: Dict[Path, bool] = {}

    for chunk_idx, df in enumerate(pd.read_csv(hrm_path, usecols=usecols, dtype=dtype_map,
                                               chunksize=chunksize, compression="gzip")):
        if enable_filters:
            df = basic_sanity_filters_hrm(df)
        else:
            df = df.dropna(subset=["deviceId", "ts", "HR"])

        if df.empty:
            continue

        if only_devices is not None:
            df = df[df["deviceId"].isin(only_devices)]
            if df.empty:
                continue

        for dev, g in df.groupby("deviceId", sort=False):
            dev_str = str(dev)
            out_dir = out_root / dev_str
            out_fp = out_dir / "all_days_hrm.csv.gz"

            g = g[["deviceId", "ts", "HR"]]

            write_chunk_to_gz(g, out_fp, header_written)

            if summary_counter is not None:
                summary_counter[dev_str] = summary_counter.get(dev_str, 0) + len(g)

        if (chunk_idx + 1) % 10 == 0:
            print(f"[HRM] Processed {chunk_idx + 1} chunks...")

    print("[HRM] Finished splitting by device.")


# ------------------------------ CLI ------------------------------- #

def main():
    parser = argparse.ArgumentParser(description="Split mixed PPG/HRM CSV.GZ files into per-device gzipped CSVs.")
    parser.add_argument("--ppg", type=str, required=True, help="Path to ppg.csv.gz")
    parser.add_argument("--hrm", type=str, required=True, help="Path to hrm.csv.gz")
    parser.add_argument("--out", type=str, default=DEFAULT_OUT_ROOT, help="Output root directory (default: data_per_user)")
    parser.add_argument("--chunksize", type=int, default=DEFAULT_CHUNKSIZE, help="Rows per chunk (default: 1,000,000)")
    parser.add_argument("--no-filter", action="store_true", help="Disable basic sanity filters")
    parser.add_argument("--devices", type=str, default="", help="Optional comma-separated deviceId whitelist")
    args = parser.parse_args()

    ppg_path = Path(args.ppg)
    hrm_path = Path(args.hrm)
    out_root = Path(args.out)
    ensure_dir(out_root)

    only_devices = None
    if args.devices.strip():
        only_devices = set(x.strip() for x in args.devices.split(",") if x.strip())

    enable_filters = not args.no_filter

    # Simple counters to report how many rows were written per device (optional)
    ppg_counter: Dict[str, int] = {}
    hrm_counter: Dict[str, int] = {}

    print("[Info] Starting PPG processing...")
    process_ppg_file(
        ppg_path=ppg_path,
        out_root=out_root,
        chunksize=args.chunksize,
        enable_filters=enable_filters,
        only_devices=only_devices,
        summary_counter=ppg_counter,
    )

    print("[Info] Starting HRM processing...")
    process_hrm_file(
        hrm_path=hrm_path,
        out_root=out_root,
        chunksize=args.chunksize,
        enable_filters=enable_filters,
        only_devices=only_devices,
        summary_counter=hrm_counter,
    )

    # Optional summary
    if ppg_counter or hrm_counter:
        print("\n[Summary] Rows written per deviceId:")
        all_devs = set(ppg_counter) | set(hrm_counter)
        for dev in sorted(all_devs):
            ppg_n = ppg_counter.get(dev, 0)
            hrm_n = hrm_counter.get(dev, 0)
            print(f"  {dev}: PPG={ppg_n:,} | HRM={hrm_n:,}")

    print("\nDone. Per-user files are under:", out_root.resolve())


if __name__ == "__main__":
    main()
