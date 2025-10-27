
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
# - filtering on time before the joins

query1 = f"""
WITH HR_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    AVG(HR) as interval_HR
FROM hrm
WHERE ts BETWEEN TIMESTAMP '2021-03-14 00:00:00' AND TIMESTAMP '2021-03-21 23:59:59'
GROUP BY time_interval),

GYR_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    AVG(x*x + y*y + z*z) AS gyr_magnitude,
FROM gyr
WHERE ts BETWEEN TIMESTAMP '2021-03-14 00:00:00' AND TIMESTAMP '2021-03-21 23:59:59'
GROUP BY time_interval),

ACC_intervals AS
(SELECT 
    time_bucket(INTERVAL '5m', ts) AS time_interval,
    AVG(x*x + y*y + z*z) AS acc_magnitude,
FROM acc
WHERE ts BETWEEN TIMESTAMP '2021-03-14 00:00:00' AND TIMESTAMP '2021-03-21 23:59:59'
GROUP BY time_interval)

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
FROM HR_intervals as h 
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
ORDER BY h.time_interval;
"""

# === CTE
# DESCRIPTION: 
# - saves precomputed values
# - filtering mid JOIN
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
GROUP BY time_interval)

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
FROM HR_intervals as h 
JOIN ACC_intervals a ON h.time_interval = a.time_interval AND h.time_interval BETWEEN TIMESTAMP '2021-03-14 00:00:00' AND TIMESTAMP '2021-03-21 23:59:59'
JOIN GYR_intervals g ON h.time_interval = g.time_interval AND g.time_interval BETWEEN TIMESTAMP '2021-03-14 00:00:00' AND TIMESTAMP '2021-03-21 23:59:59'
ORDER BY h.time_interval;
"""

# === CTE
# DESCRIPTION: 
# - saves precomputed values
# - filtering after on TIME
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
GROUP BY time_interval)

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
FROM HR_intervals as h 
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
WHERE h.time_interval BETWEEN TIMESTAMP '2021-03-14 00:00:00' AND TIMESTAMP '2021-03-21 23:59:59'
ORDER BY h.time_interval;
"""

out_dir = "output_category"
os.makedirs("profiles", exist_ok=True)

db_path = "../db_ba30/ba30_data.duckdb"
con = duckdb.connect(database=db_path)
cols, rows = None, []

queries = {"q1": query1,"q2": query2, "q3":query3}


for q, stmt in queries.items():
    print("executing:", q)
    df = con.execute(f"{stmt}").fetch_df()
    df.to_csv(f"{out_dir}/{q.upper()}_category_duckdb.csv", index=False)

con.close()
