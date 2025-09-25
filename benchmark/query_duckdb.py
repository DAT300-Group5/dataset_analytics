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

    con = duckdb.connect(db_path)

    result = None
    with open(query_file, 'r') as f:
        query = f.read()
    for stmt in query.split(';'):
        stmt = stmt.strip()
        if stmt:
            result = con.execute(stmt).fetchall()
    
    con.close()
    return len(result)