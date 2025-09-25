#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DuckDB Query Module
Contains DuckDB-related query logic and aggregation operation examples
"""

import duckdb

def run_query_duckdb(query_file, db_path=None):
    """
    DuckDB query example: create table, insert data, execute column aggregation query
    """

    # Use provided path or default
    if db_path is None:
        db_path = './data_duckdb.db'
    
    con = duckdb.connect(db_path)

    with open(query_file, 'r') as f:
        query = f.read()
    result = con.execute(query).fetchall()
    
    con.close()
    return len(result)