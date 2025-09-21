#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite Query Module
Contains SQLite-related query logic and aggregation operation examples
"""

def run_query_sqlite():
    """
    SQLite query example: create table, insert data, execute column aggregation query
    """
    import sqlite3
    
    con = sqlite3.connect('./data_sqlite.db')

    # Execute column aggregation query
    result = con.execute("""
        SELECT 
            AVG(x) as avg_x
        FROM data
    """).fetchall()
    
    con.close()
    return len(result)