
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3
import os, csv

# ===========================================================
# ==== ANOMALY DETECTION - stress, health issues ============
# ===========================================================

# SCENARIO: 
# Anomaly: when resting HR above a certain quantile (fyi 80%)
# Tables: hrm, ped
# Intervals: 1min


# Query 1 SUBOPTIMALITIES: 
# time_bucket calculate twice
# STATISTICS for HR_threshold is calculated multiple times
query1 = f"""
SELECT 
    (CAST(h.ts AS INTEGER)/60000)*60000 AS time_interval,
    AVG(h.HR) AS interval_HR, 
    MAX(p.steps) - MIN(p.steps) AS interval_steps,
    MAX(p.calories) - MIN(p.calories) AS interval_calories,
    CASE WHEN (((MAX(p.steps) - MIN(p.steps)) < 10) AND AVG(h.HR) > (SELECT AVG(HR) + exp(0.5 * ln(AVG(HR*HR)-AVG(HR)*AVG(HR))) AS threshold FROM hrm)) 
    THEN 1 ELSE 0 END AS anomaly_flag
FROM hrm AS h
JOIN ped AS p
    ON (CAST(h.ts AS INTEGER)/60000)*60000 = (CAST(p.ts AS INTEGER)/60000)*60000
GROUP BY time_interval
ORDER BY time_interval;
"""

# SUBOPTIMAL: multiple temporary tables
# OPTIMAL:  we don't calculate time buckets multiple times (when joining) 
#           we don't calculate quantile multiple times (when filtering rows)
query2 = f"""
DROP TABLE IF EXISTS PED_intervals;
DROP TABLE IF EXISTS HR_intervals;
DROP TABLE IF EXISTS HR_threshold;
DROP TABLE IF EXISTS analysis;

CREATE TEMP TABLE HR_intervals AS
SELECT 
    (CAST(ts AS INTEGER)/60000)*60000 AS time_interval,
    AVG(HR) as interval_HR
FROM hrm
GROUP BY time_interval
ORDER BY time_interval;

CREATE TEMP TABLE HR_threshold AS
SELECT AVG(HR) + exp(0.5 * ln(AVG(HR*HR)-AVG(HR)*AVG(HR))) AS threshold FROM hrm;

CREATE TEMP TABLE PED_intervals AS
SELECT 
    (CAST(ts AS INTEGER)/60000)*60000 AS time_interval,
    MAX(steps) - MIN(steps) AS interval_steps,
    MAX(calories) - MIN(calories) AS interval_calories
FROM ped
GROUP BY time_interval
ORDER BY time_interval;

SELECT 
    h.time_interval, h.interval_HR, 
    p.interval_steps, p.interval_calories,
    CASE WHEN (p.interval_steps < 10 AND h.interval_HR > q.threshold) THEN 1 ELSE 0 END AS anomaly_flag
FROM HR_intervals as h, HR_threshold as q
JOIN PED_intervals p ON h.time_interval = p.time_interval
ORDER BY h.time_interval;
"""

# USING CTE INSTEAD OF TEMP TABLES
query3 = f""" 
WITH HR_threshold AS (
    SELECT AVG(HR) + exp(0.5 * ln(AVG(HR*HR)-AVG(HR)*AVG(HR))) AS threshold FROM hrm 
),
hr_intervals AS (
    SELECT
        (CAST(ts AS INTEGER)/60000)*60000 AS time_interval,
        AVG(HR) AS interval_HR
    FROM hrm
    GROUP BY time_interval
    ORDER BY time_interval
),
ped_intervals AS (
    SELECT
        (CAST(ts AS INTEGER)/60000)*60000 AS time_interval,
        MAX(steps) - MIN(steps) AS interval_steps,
        MAX(calories) - MIN(calories) AS interval_calories
    FROM ped
    GROUP BY time_interval
    ORDER BY time_interval
)
SELECT
    h.time_interval as time_interval,
    h.interval_HR, p.interval_steps, p.interval_calories,
    CASE WHEN p.interval_steps < 10 AND h.interval_HR > q.threshold THEN 1 ELSE 0 END AS anomaly_flag
FROM hr_intervals h
JOIN ped_intervals p USING (time_interval)
CROSS JOIN HR_threshold q;
"""

# SUBOPTIMALITIES: 
#   MULTIPLE TIMES BUCKET CALC
#   INSTEAD OF TIMESTAMPS WE USED STRINGS TO COMPARE
#   CALCULATING QUANTILE EACH TIME

query4 = f"""
SELECT 
    (CAST(h.ts AS INTEGER)/60000)*60000 AS time_interval,
    AVG(h.HR) AS interval_HR, 
    MAX(p.steps) - MIN(p.steps) AS interval_steps,
    MAX(p.calories) - MIN(p.calories) AS interval_calories,

    CASE WHEN (((MAX(p.steps) - MIN(p.steps)) < 10) AND AVG(h.HR) > (SELECT AVG(HR) +  exp(0.5 * ln(AVG(HR*HR)-AVG(HR)*AVG(HR))) AS threshold FROM hrm)) 
        THEN 1 ELSE 0 END AS anomaly_flag
FROM hrm AS h
JOIN ped AS p
    ON (CAST(h.ts AS INTEGER)/60000)*60000 = (CAST(p.ts AS INTEGER)/60000)*60000
GROUP BY time_interval
ORDER BY time_interval;
"""



# ======================================================================================

"""# Recursive function to print tree
def print_tree(parent=0, indent=0):
    for id, detail in tree.get(parent, []):
        print(" " * indent + detail)
        print_tree(id, indent + 4)

db_path = "db_ba30/ba30_data.sqlite"
con = sqlite3.connect(database=db_path)
cur = con.cursor()

for q, query_sql in queries.items():
    for stmt in [s.strip() for s in query_sql.split(';') if s.strip()]:
        print("\n=== Executing", q, "\n")
        explain_file = os.path.join("profiles", f"sqlite_anomaly_{q}_explain.txt")
        cur.execute(f"EXPLAIN QUERY PLAN {stmt}")
        rows = cur.fetchall()

        with open(explain_file, "w") as f:
            for row in rows:
                f.write(str(row) + "\n")

        tree = {}
        for id, parent, _, detail in rows:
            tree.setdefault(parent, []).append((id, detail))

        print_tree()

con.close()
"""

# ==================================================================
db_path = "../db_ba30/ba30_data.sqlite"
con = sqlite3.connect(database=db_path)
cur = con.cursor()

queries = {"q1": query1, "q2": query2, "q3":query3, "q4":query4}

def save_results_to_csv(query_name, rows, folder="output_anomaly"):
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    output_file = os.path.join(folder, f"{query_name.upper()}_category_sqlite.csv")
    
    if rows:
        headers = [description[0] for description in cur.description]  
        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)  
            writer.writerows(rows)   


for q, query_sql in queries.items():
    for stmt in [s.strip() for s in query_sql.split(';') if s.strip()]:
        print("Executing", q, "\n")
        cur.execute(stmt)
        query_results = cur.fetchall()
        save_results_to_csv(q, query_results)
        
cur.close()
con.close()
