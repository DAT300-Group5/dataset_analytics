
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3
import os, csv 

db_path = "db_ba30/ba30_data.sqlite"

# ===========================================================
# ==== ACTIVITY CLASSIFICATIONS 
# # ===========================================================

# === CTE
# DESCRIPTION: 
# - saves precomputed values
# - filtering on daylight after the join

query1 = f"""
WITH HR_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(HR) AS interval_HR
FROM hrm
GROUP BY time_interval
),

GYR_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(x*x + y*y + z*z) AS gyr_magnitude
FROM gyr
GROUP BY time_interval
),

ACC_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc
GROUP BY time_interval
),

LIT_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval, 
    CASE WHEN AVG(ambient_light_intensity) > 100 THEN 1 ELSE 0 END AS is_light
FROM lit
GROUP BY time_interval
)

SELECT 
h.time_interval, h.interval_HR, 
a.acc_magnitude, g.gyr_magnitude,
CASE 
    WHEN h.interval_HR < 80 OR (a.acc_magnitude < 2 and g.gyr_magnitude < 2) THEN 'sitting' 
    WHEN h.interval_HR < 110 OR (a.acc_magnitude < 10 and g.gyr_magnitude < 10) THEN 'light_activity'
    WHEN h.interval_HR >= 110 OR (a.acc_magnitude < 100 and g.gyr_magnitude < 100) THEN 'heavy_activity'
    ELSE 'misc'
END AS type_of_activity
FROM HR_intervals h
JOIN LIT_intervals l ON h.time_interval = l.time_interval 
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
WHERE l.is_light = 1
ORDER BY h.time_interval;
"""

# === CTE
# DESCRIPTION: 
# - saves precomputed values
# - filtering during the join
query2 = f"""
WITH HR_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(HR) AS interval_HR
FROM hrm
GROUP BY time_interval
),

GYR_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(x*x + y*y + z*z) AS gyr_magnitude
FROM gyr
GROUP BY time_interval
),

ACC_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc
GROUP BY time_interval
),

LIT_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval, 
    CASE WHEN AVG(ambient_light_intensity) > 100 THEN 1 ELSE 0 END AS is_light
FROM lit
GROUP BY time_interval
)

SELECT 
h.time_interval, h.interval_HR, 
a.acc_magnitude, g.gyr_magnitude,
CASE 
    WHEN h.interval_HR < 80 OR (a.acc_magnitude < 2 and g.gyr_magnitude < 2) THEN 'sitting' 
    WHEN h.interval_HR < 110 OR (a.acc_magnitude < 10 and g.gyr_magnitude < 10) THEN 'light_activity'
    WHEN h.interval_HR >= 110 OR (a.acc_magnitude < 100 and g.gyr_magnitude < 100) THEN 'heavy_activity'
    ELSE 'misc'
END AS type_of_activity
FROM HR_intervals h
JOIN LIT_intervals l ON h.time_interval = l.time_interval AND l.is_light = 1
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
ORDER BY h.time_interval;
"""


# === CTE
# DESCRIPTION: 
# - saves precomputed values
# - filtering on daylight before join by not including the non daylight rows in the CTE

query3 = f"""
WITH HR_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(HR) AS interval_HR
FROM hrm
GROUP BY time_interval),

GYR_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(x*x + y*y + z*z) AS gyr_magnitude
FROM gyr
GROUP BY time_interval),

ACC_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc
GROUP BY time_interval
),

LIT_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,
    AVG(ambient_light_intensity) AS avg_light_intensity
FROM lit
GROUP BY time_interval
HAVING AVG(ambient_light_intensity) > 100
)

SELECT 
h.time_interval, h.interval_HR, 
a.acc_magnitude, g.gyr_magnitude,
CASE 
    WHEN h.interval_HR < 80 OR (a.acc_magnitude < 2 AND g.gyr_magnitude < 2) THEN 'sitting' 
    WHEN h.interval_HR < 110 OR (a.acc_magnitude < 10 AND g.gyr_magnitude < 10) THEN 'light_activity'
    WHEN h.interval_HR >= 110 OR (a.acc_magnitude < 100 AND g.gyr_magnitude < 100) THEN 'heavy_activity'
    ELSE 'misc'
END AS type_of_activity
FROM HR_intervals h
JOIN LIT_intervals l ON h.time_interval = l.time_interval
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
ORDER BY h.time_interval;
"""

# CTE
# DESCRIPTION: 
# - saves precomputed values
# - each expression already joins on LIT so less rows for the final join
query4 = f"""
WITH LIT_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,
    AVG(ambient_light_intensity) AS avg_light_intensity
FROM lit
GROUP BY time_interval
HAVING AVG(ambient_light_intensity) > 100),

HR_intervals AS
(SELECT 
    l.time_interval,
    AVG(h.HR) as interval_HR
FROM hrm h JOIN LIT_intervals l ON (CAST(h.ts AS INTEGER) / 300000) * 300000 = l.time_interval
GROUP BY time_interval),

GYR_intervals AS
(SELECT 
    l.time_interval,
    AVG(x*x + y*y + z*z) AS gyr_magnitude
FROM gyr g JOIN LIT_intervals l ON (CAST(g.ts AS INTEGER) / 300000) * 300000 = l.time_interval
GROUP BY time_interval),

ACC_intervals AS
(SELECT 
    l.time_interval,
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc a JOIN LIT_intervals l ON (CAST(a.ts AS INTEGER) / 300000) * 300000 = l.time_interval
GROUP BY time_interval)

SELECT 
    h.time_interval, h.interval_HR, 
    a.acc_magnitude, g.gyr_magnitude,
    CASE 
        WHEN h.interval_HR < 80 OR (a.acc_magnitude < 2 and g.gyr_magnitude < 2) THEN 'sitting' 
        WHEN h.interval_HR < 110 OR (a.acc_magnitude < 10 and g.gyr_magnitude < 10) THEN 'light_activity'
        WHEN h.interval_HR >= 110 OR (a.acc_magnitude < 100 and g.gyr_magnitude < 100) THEN 'heavy_activity'
        ELSE 'misc'
        END AS type_of_activity
FROM HR_intervals as h 
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
ORDER BY h.time_interval;
"""

# =======================================================================
"""queries = {"q1": query1, "q2": query2, "q3":query3, "q4":query4}

con = sqlite3.connect(database=db_path)
cursor = con.cursor()

for q, stmt in queries.items():
    explain_file = os.path.join("profiles", f"sqlite_{q}_explain.txt")
    cursor.execute(f"EXPLAIN QUERY PLAN {stmt}")
    plan = cursor.fetchall()

    with open(explain_file, "w") as f:
        for row in plan:
            f.write(str(row) + "\n")

con.close()"""

# ==================================================================
db_path = "../db_ba30/ba30_data.sqlite"
con = sqlite3.connect(database=db_path)
cur = con.cursor()

queries = {"q1": query1, "q2": query2, "q3":query3, "q4":query4}
# ==================================================================

def save_results_to_csv(query_name, rows, folder="output_category"):
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    output_file = os.path.join(folder, f"{query_name.upper()}_category_sqlite.csv")
    
    if rows:
        headers = [description[0] for description in cur.description]  # Get column headers from cursor description
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


