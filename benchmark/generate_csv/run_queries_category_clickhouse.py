
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, os, csv, chdb

# ===========================================================
# ==== ACTIVITY CLASSIFICATIONS - 
# # ===========================================================

# === CTE
# DESCRIPTION: 
# - saves precomputed values
# - filtering before the join
query1 = f"""
USE sensor;
WITH HR_intervals AS
(SELECT 
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
    AVG(HR) as interval_HR
FROM hrm
WHERE ts BETWEEN toDateTime('2021-03-14 00:00:00') AND toDateTime('2021-03-21 23:59:59')
GROUP BY time_interval),

GYR_intervals AS
(SELECT 
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
    AVG(x*x + y*y + z*z) AS gyr_magnitude
FROM gyr
WHERE ts BETWEEN toDateTime('2021-03-14 00:00:00') AND toDateTime('2021-03-21 23:59:59')
GROUP BY time_interval),

ACC_intervals AS
(SELECT 
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc
WHERE ts BETWEEN toDateTime('2021-03-14 00:00:00') AND toDateTime('2021-03-21 23:59:59')
GROUP BY time_interval)

SELECT 
    h.time_interval as time_interval, h.interval_HR, 
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
ORDER BY h.time_interval
INTO OUTFILE 'output_category/Q1_category_chdb.csv' FORMAT CSVWithNames;
"""

# === CTE
# DESCRIPTION: 
# - saves precomputed values
# - filtering during the join
query2 = f"""
USE sensor;
WITH HR_intervals AS
(SELECT 
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
    AVG(HR) as interval_HR
FROM hrm
GROUP BY time_interval),

GYR_intervals AS
(SELECT 
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
    AVG(x*x + y*y + z*z) AS gyr_magnitude
FROM gyr
GROUP BY time_interval),

ACC_intervals AS
(SELECT 
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc
GROUP BY time_interval)

SELECT 
    h.time_interval AS time_interval, h.interval_HR, 
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
JOIN ACC_intervals a ON h.time_interval = a.time_interval AND h.time_interval BETWEEN toDateTime('2021-03-14 00:00:00') AND toDateTime('2021-03-21 23:59:59') 
JOIN GYR_intervals g ON h.time_interval = g.time_interval
ORDER BY h.time_interval
INTO OUTFILE 'output_category/Q2_category_chdb.csv' FORMAT CSVWithNames;
"""



# === CTE
# DESCRIPTION: 
# - saves precomputed values
# - filtering after the join

query3 = f"""
USE sensor;
WITH HR_intervals AS
(SELECT 
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
    AVG(HR) as interval_HR
FROM hrm
GROUP BY time_interval),

GYR_intervals AS
(SELECT 
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
    AVG(x*x + y*y + z*z) AS gyr_magnitude
FROM gyr
GROUP BY time_interval),

ACC_intervals AS
(SELECT 
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc
GROUP BY time_interval)

SELECT 
    h.time_interval AS time_interval, h.interval_HR, 
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
WHERE h.time_interval BETWEEN toDateTime('2021-03-14 00:00:00') AND toDateTime('2021-03-21 23:59:59')
ORDER BY h.time_interval
INTO OUTFILE 'output_category/Q3_category_chdb.csv' FORMAT CSVWithNames;
"""


# ==================================================================
db_path = "../db_ba30/ba30_data_chdb"
con = chdb.connect(db_path)
con.query("USE sensor;")

queries = {"q1": query1, "q2": query2, "q3": query3}

output_dir = "output_category"
os.makedirs(output_dir, exist_ok=True)

for name, q in queries.items():
    print(f"Executing {name} ...")
    output_path = os.path.join(output_dir, f"{name}_category_chdb.csv")
    try:
        res = con.query(q)
    except Exception as e:
        print(f"Failed to execute {name}: {e}")

con.close()