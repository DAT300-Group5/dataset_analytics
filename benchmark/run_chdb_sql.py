#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, csv, chdb

from utils import load_query_from_file, split_statements

if len(sys.argv) < 3:
    sys.exit(f"Usage: {sys.argv[0]} DB_PATH SQL_FILE")

db_path, sql_file = sys.argv[1], sys.argv[2]
sql = load_query_from_file(sql_file)

con = chdb.connect(db_path)
cur = con.cursor()
cols, rows = None, []
for stmt in split_statements(sql):
    cur.execute(stmt)
    if cur.description:
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
cur.close()
con.close()

if cols:
    w = csv.writer(sys.stdout)
    w.writerow(cols)
    w.writerows(rows)
