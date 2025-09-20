#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Split a single raw sensor file (ppg, hrm, acc, grv, gyr, lit, ped)
into per-user gzipped CSVs under data_per_user/<deviceId>/.

No cleaning is performed. This script groups by deviceId and appends
rows into per-user files, while also collecting progress statistics.

Input (gzipped CSV):
  - Specified sensor file: e.g., ppg.csv.gz, hrm.csv.gz, etc.

Output:
  data_per_user/<deviceId>/<sensor>.csv.gz

Summary:
  - Per-sensor total rows written
  - Per-sensor per-device row counts
  - Cross-sensor per-device total row counts (for this sensor)
"""

import gzip
import argparse
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


# ----------------------------- Config ----------------------------- #

DEFAULT_OUT_ROOT = "data_per_user"
DEFAULT_CHUNKSIZE = 1_000_000  # rows per chunk


# --------------------------- Utilities ---------------------------- #

def ensure_dir(p: Path) -> None:
    """Create directory if it does not exist."""
    p.mkdir(parents=True, exist_ok=True)


def open_gz_append(path: Path):
    """Open a gzip file for appending (multi-member gzip)."""
    return gzip.open(path, mode="ab")


def write_chunk_to_gz(
    df: pd.DataFrame,
    out_path: Path,
    header_written_cache: Dict[Path, bool],
) -> None:
    """Append DataFrame to gzipped CSV file, with header written once."""
    ensure_dir(out_path.parent)
    write_header = not header_written_cache.get(out_path, False) and not out_path.exists()

    csv_bytes = df.to_csv(index=False, header=write_header).encode("utf-8")
    with open_gz_append(out_path) as f:
        f.write(csv_bytes)

    header_written_cache[out_path] = True


def split_file_by_device(
    input_path: Path,
    out_root: Path,
    output_name: str,
    usecols: List[str],
    sensor_key: str,
    chunksize: int = DEFAULT_CHUNKSIZE,
    only_devices: Optional[set] = None,
    # summaries (mutated in-place)
    per_sensor_device_counts: Optional[Dict[str, Dict[str, int]]] = None,
    per_sensor_totals: Optional[Dict[str, int]] = None,
) -> None:
    """
    Stream a gzipped CSV and split into per-device files, updating summary stats.

    Args:
        input_path: path to input gzipped CSV
        out_root: output root directory (per-device subdirs)
        output_name: filename saved under each device folder
        usecols: columns to keep
        sensor_key: key for this sensor (e.g., 'ppg', 'hrm')
        chunksize: rows per chunk
        only_devices: optional whitelist of deviceIds
        per_sensor_device_counts: nested dict[sensor_key][deviceId] -> rows
        per_sensor_totals: dict[sensor_key] -> total rows
    """
    print(f"[Info] Processing {input_path.name} -> {output_name}")
    header_written: Dict[Path, bool] = {}

    # init summary holders
    if per_sensor_device_counts is not None and sensor_key not in per_sensor_device_counts:
        per_sensor_device_counts[sensor_key] = {}
    if per_sensor_totals is not None and sensor_key not in per_sensor_totals:
        per_sensor_totals[sensor_key] = 0

    for chunk_idx, df in enumerate(pd.read_csv(
        input_path,
        usecols=usecols,
        chunksize=chunksize,
        compression="gzip",
        dtype=str,   # do not convert types; keep as-is
    )):
        # optional device filter
        if only_devices is not None:
            df = df[df["deviceId"].isin(only_devices)]
            if df.empty:
                continue

        # write per device
        for dev, g in df.groupby("deviceId", sort=False):
            dev_str = str(dev)
            out_fp = out_root / dev_str / output_name
            write_chunk_to_gz(g[usecols], out_fp, header_written)

            # update per-sensor per-device counts
            if per_sensor_device_counts is not None:
                per_sensor_device_counts[sensor_key][dev_str] = (
                    per_sensor_device_counts[sensor_key].get(dev_str, 0) + len(g)
                )

        # update per-sensor total
        if per_sensor_totals is not None:
            per_sensor_totals[sensor_key] += len(df)

        if (chunk_idx + 1) % 10 == 0:
            print(f"  processed {chunk_idx + 1} chunks...")

    print(f"[Info] Finished {input_path.name}")


def print_summary(
    per_sensor_totals: Dict[str, int],
    per_sensor_device_counts: Dict[str, Dict[str, int]],
    top_n: int = 20,
) -> None:
    """Pretty-print summary statistics."""
    print("\n================ SUMMARY ================")
    # 1) per-sensor totals
    print("\n[Totals by sensor]")
    grand_total = 0
    for sensor in sorted(per_sensor_totals.keys()):
        total = per_sensor_totals[sensor]
        grand_total += total
        print(f"  {sensor:>4}: {total:,} rows")
    print(f"  ----\n  ALL : {grand_total:,} rows")

    # 2) per-sensor per-device (top N per sensor)
    print("\n[Top devices per sensor]")
    for sensor in sorted(per_sensor_device_counts.keys()):
        dev_map = per_sensor_device_counts[sensor]
        items = sorted(dev_map.items(), key=lambda kv: kv[1], reverse=True)
        print(f"\n  {sensor}: (top {min(top_n, len(items))})")
        for dev, cnt in items[:top_n]:
            print(f"    {dev}: {cnt:,}")

    # 3) cross-sensor totals per device (top N overall)
    print("\n[Top devices across ALL sensors]")
    overall: Dict[str, int] = {}
    for sensor, dev_map in per_sensor_device_counts.items():
        for dev, cnt in dev_map.items():
            overall[dev] = overall.get(dev, 0) + cnt
    items_all = sorted(overall.items(), key=lambda kv: kv[1], reverse=True)
    for dev, cnt in items_all[:top_n]:
        print(f"  {dev}: {cnt:,}")
    print("=========================================\n")


# ------------------------------ CLI ------------------------------- #

def main():
    parser = argparse.ArgumentParser(description="Split a single sensor CSV.GZ file into per-device gzipped CSVs (with progress summary).")
    parser.add_argument("--input", type=str, required=True, help="Path to input sensor CSV.GZ file")
    parser.add_argument("--sensor", type=str, required=True, choices=["ppg", "hrm", "acc", "grv", "gyr", "lit", "ped"], help="Sensor type")
    parser.add_argument("--out", type=str, default=DEFAULT_OUT_ROOT, help="Output root directory")
    parser.add_argument("--chunksize", type=int, default=DEFAULT_CHUNKSIZE, help="Rows per chunk (default: 1,000,000)")
    parser.add_argument("--devices", type=str, default="", help="Optional comma-separated deviceId whitelist")
    parser.add_argument("--top", type=int, default=20, help="How many top devices to list in the summary (default: 20)")
    args = parser.parse_args()

    out_root = Path(args.out)
    ensure_dir(out_root)

    only_devices = None
    if args.devices.strip():
        only_devices = set(x.strip() for x in args.devices.split(",") if x.strip())

    # sensor configurations
    sensor_configs = {
        "ppg": ("all_days_ppg.csv.gz", ["deviceId", "ts", "ppg"]),
        "hrm": ("all_days_hrm.csv.gz", ["deviceId", "ts", "HR"]),
        "acc": ("all_days_acc.csv.gz", ["deviceId", "ts", "x", "y", "z"]),
        "grv": ("all_days_grv.csv.gz", ["deviceId", "ts", "x", "y", "z", "w"]),
        "gyr": ("all_days_gyr.csv.gz", ["deviceId", "ts", "x", "y", "z"]),
        "lit": ("all_days_lit.csv.gz", ["deviceId", "ts", "ambient_light_intensity"]),
        "ped": ("all_days_ped.csv.gz", ["deviceId", "ts", "steps", "steps_walking", "steps_running", "distance", "calories"]),
    }

    if args.sensor not in sensor_configs:
        raise ValueError(f"Invalid sensor type: {args.sensor}")

    output_name, usecols = sensor_configs[args.sensor]

    # summary holders
    per_sensor_device_counts: Dict[str, Dict[str, int]] = {}
    per_sensor_totals: Dict[str, int] = {}

    # process single file
    split_file_by_device(
        input_path=Path(args.input),
        out_root=out_root,
        output_name=output_name,
        usecols=usecols,
        sensor_key=args.sensor,
        chunksize=args.chunksize,
        only_devices=only_devices,
        per_sensor_device_counts=per_sensor_device_counts,
        per_sensor_totals=per_sensor_totals,
    )

    # print summary
    print_summary(
        per_sensor_totals=per_sensor_totals,
        per_sensor_device_counts=per_sensor_device_counts,
        top_n=args.top,
    )

    print("Done. Per-user files are under:", out_root.resolve())


if __name__ == "__main__":
    main()
