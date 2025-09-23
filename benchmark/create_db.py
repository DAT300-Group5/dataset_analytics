import sqlite3

import pandas as pd

def create(source_path, target_path, engine='duckdb'):
    df = pd.read_csv(source_path)
    # Save as new CSV file
    if engine == 'duckdb':
        import duckdb
        conn = duckdb.connect(target_path)
        conn.execute("CREATE TABLE data AS SELECT * FROM df")
        print(conn.execute("SELECT COUNT(*) FROM data").fetchall())
        conn.close()
    elif engine == 'sqlite':
        import sqlite3
        conn = sqlite3.connect(target_path)
        df.to_sql('data', conn, index=False, if_exists='replace')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM data")
        print(cursor.fetchall())
        conn.close()
    print(f"Processed data saved to {target_path}")

if __name__ == "__main__":
    conn = sqlite3.connect('./data_sqlite.db')
    cursor = conn.cursor()
    cursor.execute("SELECT deviceId FROM data")
    print(cursor.fetchall())

    conn.close()
