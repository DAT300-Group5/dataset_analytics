#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite Query Module
Contains SQLite-related query logic and aggregation operation examples
"""

import sqlite3

def run_query_sqlite(query_file, db_path=None):
    """
    SQLite query example: create table, insert data, execute column aggregation query
    """

    con = sqlite3.connect(db_path)

    result = None
    with open(query_file, 'r') as f:
        query = f.read()
    for stmt in query.split(';'):
        stmt = stmt.strip()
        if stmt:
            result = con.execute(stmt).fetchall()
    
    con.close()
    return len(result)