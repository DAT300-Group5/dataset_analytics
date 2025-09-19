import numpy as np
import pandas as pd 

import pyarrow as pa 
import pyarrow.parquet as pq

import datasketches as ds

import duckdb
import sqlite3
import warnings

csv_filename = 'data/sleep_diary.csv'

# PRPEPROCESS ORIGINAL DATA ===================

def load_data(filename):
    df = pd.read_csv(filename)
    #print(df.head(5))
    #print(df.describe())
    print(df.columns)
    return df

def missing_values_check(df):
    print(df.isna().sum())
    return df

def datatypes_conversion(df):
    df['date'] = pd.to_datetime(df['date'])
    float_cols = ['waso', 'sleep_duration', 'in_bed_duration', 'sleep_latency', 'sleep_efficiency']
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=[col])
    return df

def preprocess_data(filename):
    df = load_data(filename)
    df = missing_values_check(df)
    df = datatypes_conversion(df)
    return df

# CONVERT TO DATA FORMATS ========================

# --- Parquet 
def convert_df_to_parquet(df, save_to_path):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, save_to_path)

# CREATE DATABASE TABLE ==========================

# --- DuckDB 
def create_duckdb_table(db_name, table_name, source, source_name, source_type):
    con = duckdb.connect(db_name)
    if source_type=='df':
        print("Creating DuckDB table from a df.")
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        create_query = f"CREATE TABLE {table_name} AS SELECT * FROM {source_name}"
        con.register(source_name, source)
        con.execute(create_query)

    elif source_type=="csv":
        print("Creating DuckDB table from CSV file.")
        # TODO

    print("Checking if the table was created.")
    res = con.execute(f"SELECT * FROM {table_name} LIMIT 10").fetchall()
    #print(res)
    con.close()

# --- SQLite 
def create_sqlite_table(db_name, table_name, source, source_type):
    con = sqlite3.connect(db_name)
    if source_type=='df':
        print("Creating SQLite table from a df.")
        source.to_sql(table_name, con, if_exists="replace", index=False)

    elif source_type=="csv":
        print("Creating SQLite table from CSV file.")
        # TODO

    print("Checking if the table was created.")
    res = con.execute(f"SELECT * FROM {table_name} LIMIT 10").fetchall()
    print(res)
    con.close()


# QUERY ==============================================

# --- SQLite
def query_sqlite_db(database_name, table_name, query):
    con = sqlite3.connect(database_name)
    cur = con.cursor()
    cur.execute(query)
    res = cur.fetchall()
    con.close()
    return res




# TESTING =============================================


# --- DuckDB


sleep_data = preprocess_data(csv_filename)
create_duckdb_table("test_duckdb_db.duckdb", "test_table",  sleep_data, "df", "df")
        

