
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
WHERE ts BETWEEN 1615680000 AND 1616371199
GROUP BY time_interval
),

GYR_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(x*x + y*y + z*z) AS gyr_magnitude
FROM gyr
WHERE ts BETWEEN 1615680000 AND 1616371199
GROUP BY time_interval
),

ACC_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc
WHERE ts BETWEEN 1615680000 AND 1616371199
GROUP BY time_interval
)

SELECT 
h.time_interval, h.interval_HR, 
a.acc_magnitude, g.gyr_magnitude,
CASE
    WHEN h.interval_HR < 80 THEN
        CASE WHEN a.acc_magnitude < 90 AND g.gyr_magnitude < 5000 THEN 'sitting'
        ELSE 'light_activity' END

    WHEN h.interval_HR < 110 THEN
        CASE WHEN a.acc_magnitude < 90 AND g.gyr_magnitude < 5000 THEN 'sitting'
        ELSE 'light_activity' END

    WHEN h.interval_HR >= 110 THEN 
        CASE WHEN a.acc_magnitude > 110 AND g.gyr_magnitude > 8000 THEN 'heavy_activity'
        ELSE 'light_activity' END
    ELSE 'misc'  
END AS type_of_activity  
FROM HR_intervals h
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
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
WHERE ts BETWEEN 1615680000 AND 1616371199
GROUP BY time_interval
),

ACC_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc
GROUP BY time_interval
)

SELECT 
h.time_interval, h.interval_HR, 
a.acc_magnitude, g.gyr_magnitude,
CASE
    WHEN h.interval_HR < 80 THEN
        CASE WHEN a.acc_magnitude < 90 AND g.gyr_magnitude < 5000 THEN 'sitting'
        ELSE 'light_activity' END

    WHEN h.interval_HR < 110 THEN
        CASE WHEN a.acc_magnitude < 90 AND g.gyr_magnitude < 5000 THEN 'sitting'
        ELSE 'light_activity' END

    WHEN h.interval_HR >= 110 THEN 
        CASE WHEN a.acc_magnitude > 110 AND g.gyr_magnitude > 8000 THEN 'heavy_activity'
        ELSE 'light_activity' END
    ELSE 'misc'  
END AS type_of_activity  
FROM HR_intervals h
JOIN ACC_intervals a ON h.time_interval = a.time_interval AND h.time_interval BETWEEN 1615680000 AND 1616371199
JOIN GYR_intervals g ON h.time_interval = g.time_interval
ORDER BY h.time_interval;
"""


# === CTE
# DESCRIPTION: 
# - saves precomputed values
# - filtering before join 

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
WHERE ts BETWEEN 1615680000 AND 1616371199
GROUP BY time_interval),

ACC_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc
WHERE ts BETWEEN 1615680000 AND 1616371199
GROUP BY time_interval
)

SELECT 
h.time_interval, h.interval_HR, 
a.acc_magnitude, g.gyr_magnitude,
CASE
    WHEN h.interval_HR < 80 THEN
        CASE WHEN a.acc_magnitude < 90 AND g.gyr_magnitude < 5000 THEN 'sitting'
        ELSE 'light_activity' END

    WHEN h.interval_HR < 110 THEN
        CASE WHEN a.acc_magnitude < 90 AND g.gyr_magnitude < 5000 THEN 'sitting'
        ELSE 'light_activity' END

    WHEN h.interval_HR >= 110 THEN 
        CASE WHEN a.acc_magnitude > 110 AND g.gyr_magnitude > 8000 THEN 'heavy_activity'
        ELSE 'light_activity' END
    ELSE 'misc'  
END AS type_of_activity  
FROM HR_intervals h
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
WHERE h.time_interval BETWEEN 1615680000 AND 1616371199
ORDER BY h.time_interval;
"""


# ==================================================================
db_path = "../db_ba30/ba30_data.sqlite"
con = sqlite3.connect(database=db_path)
cur = con.cursor()

queries = {"q1": query1, "q2": query2, "q3":query3}
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
        print("Executing", q)
        cur.execute(stmt)
        query_results = cur.fetchall()
        save_results_to_csv(q, query_results)

cur.close()
con.close()


