#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, csv, chdb

if len(sys.argv) < 3:
    sys.exit(f"Usage: {sys.argv[0]} DB_PATH SQL_FILE")

db_path, sql_file = sys.argv[1], sys.argv[2]
sql = open(sql_file, "r", encoding="utf-8").read()

conn = chdb.connect(db_path)
cur = conn.cursor()
cols, rows = None, []
for stmt in [s.strip() for s in sql.split(';') if s.strip()]:
    cur.execute(stmt)
    if cur.description:
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
cur.close()
conn.close()

if cols:
    w = csv.writer(sys.stdout)
    w.writerow(cols)
    w.writerows(rows)
