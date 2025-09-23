#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DuckDB Query Module
Contains DuckDB-related query logic and aggregation operation examples
"""

def run_query_duckdb(db_path=None):
    """
    DuckDB query example: create table, insert data, execute column aggregation query
    """
    import duckdb
    
    # Use provided path or default
    if db_path is None:
        db_path = './data_duckdb.db'
    
    con = duckdb.connect(db_path)

    # Execute column aggregation query
    result = con.execute("""
        SELECT 
            AVG(x) as avg_x
        FROM acc
    """).fetchall()
    
    con.close()
    return len(result)