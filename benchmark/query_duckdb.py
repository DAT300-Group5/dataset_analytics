#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DuckDB Query Module
Contains DuckDB-related query logic and aggregation operation examples
"""

def run_query_duckdb():
    """
    DuckDB query example: create table, insert data, execute column aggregation query
    """
    import duckdb
    
    con = duckdb.connect('./data_duckdb.db')

    # Execute column aggregation query
    result = con.execute("""
        SELECT 
            AVG(x) as avg_x
        FROM data
    """).fetchall()
    
    con.close()
    return len(result)