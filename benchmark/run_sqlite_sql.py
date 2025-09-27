#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, csv, sqlite3

if len(sys.argv) < 3:
    sys.exit(f"Usage: {sys.argv[0]} DB_PATH SQL_FILE")

db_path, sql_file = sys.argv[1], sys.argv[2]
sql = open(sql_file, "r", encoding="utf-8").read()

con = sqlite3.connect(db_path)
con.isolation_level = None
cur = con.cursor()
cols, rows = None, []
for stmt in [s.strip() for s in sql.split(';') if s.strip()]:
    cur.execute(stmt)
    if cur.description:
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
con.close()

if cols:
    w = csv.writer(sys.stdout)
    w.writerow(cols)
    w.writerows(rows)
