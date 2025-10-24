
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import duckdb, os
import json
import statistics
from datetime import datetime

# ===========================================================
# ==== ACTIVITY CLASSIFICATIONS - 
# # ===========================================================

# === CTE
# DESCRIPTION: 
# - saves precomputed values
# - filtering on daylight after the join
query1 = f"""
WITH HR_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    AVG(HR) as interval_HR
FROM hrm
GROUP BY time_interval),

GYR_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    AVG(x*x + y*y + z*z) AS gyr_magnitude,
FROM gyr
GROUP BY time_interval),

ACC_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    AVG(x*x + y*y + z*z) AS acc_magnitude,
FROM acc
GROUP BY time_interval),

LIT_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    CASE WHEN AVG(ambient_light_intensity) > 100 THEN 1 ELSE 0 END as is_light
FROM lit
GROUP BY time_interval)

SELECT 
    h.time_interval, h.interval_HR, 
    a.acc_magnitude, g.gyr_magnitude,
    CASE 
        WHEN h.interval_HR < 80 OR (a.acc_magnitude < 2 AND g.gyr_magnitude < 2) THEN 'sitting' 
        WHEN h.interval_HR < 110 OR (a.acc_magnitude < 10 AND g.gyr_magnitude < 10) THEN 'light_activity'
        WHEN h.interval_HR >= 110 OR (a.acc_magnitude < 100 AND g.gyr_magnitude < 100) THEN 'heavy_activity'
        ELSE 'misc' END AS type_of_activity
FROM HR_intervals as h 
JOIN LIT_intervals l ON h.time_interval = l.time_interval
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
WHERE l.is_light == 1
ORDER BY h.time_interval;
"""

# === CTE
# DESCRIPTION: 
# - saves precomputed values
# - filtering light during the join
query2 = f"""
WITH HR_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    AVG(HR) as interval_HR
FROM hrm
GROUP BY time_interval),

GYR_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    AVG(x*x + y*y + z*z) AS gyr_magnitude
FROM gyr
GROUP BY time_interval),

ACC_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc
GROUP BY time_interval),

LIT_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    CASE WHEN AVG(ambient_light_intensity) > 100 THEN 1 ELSE 0 END as is_light
FROM lit
GROUP BY time_interval)

SELECT 
    h.time_interval, h.interval_HR, 
    a.acc_magnitude, g.gyr_magnitude,
    CASE 
        WHEN h.interval_HR < 80 OR (a.acc_magnitude < 2 AND g.gyr_magnitude < 2) THEN 'sitting' 
        WHEN h.interval_HR < 110 OR (a.acc_magnitude < 10 AND g.gyr_magnitude < 10) THEN 'light_activity'
        WHEN h.interval_HR >= 110 OR (a.acc_magnitude < 100 AND g.gyr_magnitude < 100) THEN 'heavy_activity'
        ELSE 'misc' END AS type_of_activity
FROM HR_intervals as h 
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
WITH HR_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    AVG(HR) as interval_HR
FROM hrm
GROUP BY time_interval),

GYR_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    AVG(x*x + y*y + z*z) AS gyr_magnitude,
FROM gyr
GROUP BY time_interval),

ACC_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    AVG(x*x + y*y + z*z) AS acc_magnitude,
FROM acc
GROUP BY time_interval),

LIT_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
FROM lit
GROUP BY time_interval
HAVING AVG(ambient_light_intensity) > 100)

SELECT 
    h.time_interval, h.interval_HR, 
    a.acc_magnitude, g.gyr_magnitude,
    CASE 
        WHEN h.interval_HR < 80 OR (a.acc_magnitude < 2 AND g.gyr_magnitude < 2) THEN 'sitting' 
        WHEN h.interval_HR < 110 OR (a.acc_magnitude < 10 AND g.gyr_magnitude < 10) THEN 'light_activity'
        WHEN h.interval_HR >= 110 OR (a.acc_magnitude < 100 AND g.gyr_magnitude < 100) THEN 'heavy_activity'
        ELSE 'misc' END AS type_of_activity
FROM HR_intervals as h 
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
WITH LIT_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
FROM lit
GROUP BY time_interval
HAVING AVG(ambient_light_intensity) > 100),

HR_intervals AS
(SELECT 
    l.time_interval,
    AVG(h.HR) as interval_HR
FROM hrm h JOIN LIT_intervals l ON time_bucket(INTERVAL '5m', h.ts) = l.time_interval
GROUP BY time_interval),

GYR_intervals AS
(SELECT 
    l.time_interval,
    AVG(x*x + y*y + z*z) AS gyr_magnitude,
FROM gyr g JOIN LIT_intervals l ON time_bucket(INTERVAL '5m', g.ts) = l.time_interval
GROUP BY time_interval),

ACC_intervals AS
(SELECT 
    l.time_interval,
    AVG(x*x + y*y + z*z) AS acc_magnitude,
FROM acc a JOIN LIT_intervals l ON time_bucket(INTERVAL '5m', a.ts) = l.time_interval
GROUP BY time_interval)

SELECT 
    h.time_interval, h.interval_HR, 
    a.acc_magnitude, g.gyr_magnitude,
    CASE 
        WHEN h.interval_HR < 80 OR (a.acc_magnitude < 2 AND g.gyr_magnitude < 2) THEN 'sitting' 
        WHEN h.interval_HR < 110 OR (a.acc_magnitude < 10 AND g.gyr_magnitude < 10) THEN 'light_activity'
        WHEN h.interval_HR >= 110 OR (a.acc_magnitude < 100 AND g.gyr_magnitude < 100) THEN 'heavy_activity'
        ELSE 'misc' END AS type_of_activity
FROM HR_intervals as h 
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
ORDER BY h.time_interval;
"""

out_dir = "output_category"
os.makedirs("profiles", exist_ok=True)
os.makedirs("profiles", exist_ok=True)

db_path = "../db_ba30/ba30_data.duckdb"
con = duckdb.connect(database=db_path)
cols, rows = None, []

queries = {"q1": query1,"q2": query2, "q3":query3, "q4":query4}


for q, stmt in queries.items():
    print("executing:", q)
    df = con.execute(f"{stmt}").fetch_df()
    df.to_csv(f"{out_dir}/{q.upper()}_category_duckdb.csv", index=False)

con.close()
