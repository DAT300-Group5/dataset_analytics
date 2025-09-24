import duckdb
import sqlite3
import argparse
import pandas as pd

root_path = "../raw_data/"

types = ["acc", "grv", "gyr", "lit", "ped", "ppg"]

def create(target_path, device_id, engine='duckdb'):

    # Save as new CSV file
    if engine == 'duckdb':
        conn = duckdb.connect(target_path)
        for type in types:
            source_path = "{}/{}/{}_{}.csv".format(root_path,type, type, device_id)
            df = pd.read_csv(source_path)
            conn.execute("CREATE TABLE {} AS SELECT * FROM df".format(type))
        conn.close()
    elif engine == 'sqlite':
        conn = sqlite3.connect(target_path)
        for type in types:
            source_path = "{}/{}/{}_{}.csv".format(root_path,type, type, device_id)
            df = pd.read_csv(source_path)
            df.to_sql(type, conn, index=False, if_exists='replace')
        conn.close()
    print(f"Database created at {target_path} using {engine} for device {device_id}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create database from CSV files for a specific device')
    parser.add_argument('device_id', help='Device ID to process (e.g., vs14, ab60)')
    parser.add_argument('target_path', help='Path for the output database file (e.g., ./test.db)')
    parser.add_argument('--engine', choices=['duckdb', 'sqlite'], default='duckdb', 
                        help='Database engine to use (default: duckdb)')
    
    args = parser.parse_args()
    
    create(args.target_path, args.device_id, args.engine)
