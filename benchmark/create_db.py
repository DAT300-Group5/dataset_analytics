#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
create_db.py — Create a database for one device across SQLite / DuckDB / chDB.

- Reads CSVs into pandas DataFrames from:
    ../raw_data/<table>/<table>_<device_id>.csv
- Creates tables for: acc, grv, gyr, lit, ped, ppg, hrm
- Optionally executes SQL files after database creation (e.g., for creating indexes)
- Engines:
    * SQLite: pandas.DataFrame.to_sql(...)
    * DuckDB: CREATE TABLE ... AS SELECT * FROM df
    * chDB:   Session(target_path); CREATE TABLE ... ENGINE=MergeTree ORDER BY ts AS SELECT * FROM Python(df)

Usage:
  python create_db.py <device_id> <target_path> --engine {duckdb,sqlite,chdb} [--post-sql file1.sql file2.sql ...]
"""

import os
import argparse
import pandas as pd
import sqlite3
import duckdb
from chdb import session as chs  # ← direct import, no fallback

# Adjust if your data root or table list changes
root_path = "../raw_data/"
types = ["acc", "grv", "gyr", "lit", "ped", "ppg", "hrm"]


def _csv_path(table: str, device_id: str) -> str:
    return os.path.join(root_path, table, f"{table}_{device_id}.csv")


def create(target_path: str, device_id: str, engine: str = "duckdb", post_sql: list[str] = None) -> None:
    """
    Create a database with all sensor tables for a single device.
    """
    engine = engine.lower().strip()
    if engine not in {"duckdb", "sqlite", "chdb"}:
        raise ValueError(f"Unsupported engine: {engine}")

    if engine == "duckdb":
        con = duckdb.connect(target_path)
        try:
            for t in types:
                csv = _csv_path(t, device_id)
                if not os.path.exists(csv):
                    print(f"[WARN] Missing file skipped: {csv}")
                    continue
                df = pd.read_csv(csv)
                con.execute(f"DROP TABLE IF EXISTS {t}")
                con.execute(f"CREATE TABLE {t} AS SELECT * FROM df")
                print(f"[OK] DuckDB loaded: {t} rows={len(df)}")
            
            # Execute post-creation SQL files
            if post_sql:
                print(f"[INFO] Executing {len(post_sql)} SQL file(s) on DuckDB...")
                for sql_file in post_sql:
                    if not os.path.exists(sql_file):
                        print(f"[WARN] SQL file not found, skipping: {sql_file}")
                        continue
                    with open(sql_file, 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                    con.execute(sql_content)
                    print(f"[OK] DuckDB executed SQL file: {sql_file}")
        finally:
            con.close()

    elif engine == "sqlite":
        con = sqlite3.connect(target_path)
        try:
            for t in types:
                csv = _csv_path(t, device_id)
                if not os.path.exists(csv):
                    print(f"[WARN] Missing file skipped: {csv}")
                    continue
                df = pd.read_csv(csv)
                df.to_sql(t, con, index=False, if_exists="replace")
                print(f"[OK] SQLite loaded: {t} rows={len(df)}")
            
            # Execute post-creation SQL files
            if post_sql:
                print(f"[INFO] Executing {len(post_sql)} SQL file(s) on SQLite...")
                for sql_file in post_sql:
                    if not os.path.exists(sql_file):
                        print(f"[WARN] SQL file not found, skipping: {sql_file}")
                        continue
                    with open(sql_file, 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                    con.executescript(sql_content)
                    print(f"[OK] SQLite executed SQL file: {sql_file}")
        finally:
            con.close()

    else:  # chdb
        os.makedirs(target_path, exist_ok=True)
        sess = chs.Session(target_path)
        try:
            sess.query("CREATE DATABASE IF NOT EXISTS sensor ENGINE = Atomic")
            sess.query("USE sensor")
            for t in types:
                csv = _csv_path(t, device_id)
                if not os.path.exists(csv):
                    print(f"[WARN] Missing file skipped: {csv}")
                    continue
                df = pd.read_csv(csv)
                order_by = "ts" if "ts" in df.columns else "tuple()"
                sess.query(f"""
                    CREATE TABLE IF NOT EXISTS {t}
                    ENGINE = MergeTree
                    ORDER BY {order_by}
                    AS SELECT * FROM Python(df)
                """)
                print(f"[OK] chDB loaded: {t} rows={len(df)} order_by={order_by}")
            
            # Execute post-creation SQL files
            if post_sql:
                print(f"[INFO] Executing {len(post_sql)} SQL file(s) on chDB...")
                for sql_file in post_sql:
                    if not os.path.exists(sql_file):
                        print(f"[WARN] SQL file not found, skipping: {sql_file}")
                        continue
                    with open(sql_file, 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                    # Split by semicolons and execute each statement
                    statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
                    for stmt in statements:
                        sess.query(stmt)
                    print(f"[OK] chDB executed SQL file: {sql_file}")
        finally:
            try:
                sess.close()
            except Exception:
                pass

    print(f"[DONE] Database created at {target_path} using {engine} for device {device_id}")


def main():
    parser = argparse.ArgumentParser(
        description="Create database from CSV files for a specific device (SQLite/DuckDB/chDB)."
    )
    parser.add_argument("device_id", help="Device ID to process (e.g., vs14, ab60)")
    parser.add_argument(
        "target_path",
        help="Path for output: file for duckdb/sqlite; directory for chdb (e.g., ./chdb_db)",
    )
    parser.add_argument(
        "--engine",
        choices=["duckdb", "sqlite", "chdb"],
        default="duckdb",
        help="Database engine to use (default: duckdb)",
    )
    parser.add_argument(
        "--post-sql",
        nargs="*",
        help="SQL files to execute after database creation (e.g., for creating indexes)",
    )
    args = parser.parse_args()
    create(args.target_path, args.device_id, args.engine, args.post_sql)


if __name__ == "__main__":
    main()
