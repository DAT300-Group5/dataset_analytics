
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import chdb, os

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
    toStartOfMinute(h.ts) AS time_interval,
    AVG(h.HR) AS interval_HR, 
    MAX(p.steps) - MIN(p.steps) AS interval_steps,
    MAX(p.calories) - MIN(p.calories) AS interval_calories,
    CASE WHEN (interval_steps < 10 AND interval_HR > (SELECT AVG(HR) + stddevSampStable(HR) FROM hrm)) THEN 1 ELSE 0 END AS anomaly_flag
FROM hrm AS h
JOIN ped AS p
    ON toStartOfMinute(h.ts) = toStartOfMinute(p.ts)
GROUP BY toStartOfMinute(h.ts)
ORDER BY toStartOfMinute(h.ts)
INTO OUTFILE 'output_anomaly/Q1_anomaly_chdb.csv' FORMAT CSVWithNames;
"""

# SUBOPTIMAL: multiple temporary tables
# OPTIMAL:  we don't calculate time buckets multiple times (when joining) 
#           we don't calculate quantile multiple times (when filtering rows)
# clickhouse does not support temp tables
query2 = f"""
DROP TABLE IF EXISTS PED_intervals;
DROP TABLE IF EXISTS HR_intervals;
DROP TABLE IF EXISTS analysis;
DROP TABLE IF EXISTS HR_threshold;

CREATE TEMP TABLE HR_intervals AS
SELECT 
    toStartOfMinute(ts) AS time_interval,
    AVG(HR) as interval_HR
FROM hrm
GROUP BY time_interval
ORDER BY time_interval;

CREATE TABLE HR_threshold AS
SELECT AVG(HR) + stddevSampStable(HR) AS threshold FROM hrm;

CREATE TEMP TABLE PED_intervals AS
SELECT 
    toStartOfMinute(ts) AS time_interval,
    MAX(steps) - MIN(steps) AS interval_steps,
    MAX(calories) - MIN(calories) AS interval_calories
FROM ped
GROUP BY time_interval
ORDER BY time_interval;

CREATE TEMP TABLE analysis AS
SELECT 
    h.time_interval, h.interval_HR, 
    p.interval_steps, p.interval_calories,
    CASE WHEN (p.interval_steps < 10 AND h.interval_HR > hq.threshold) THEN 1 ELSE 0 END AS anomaly_flag
FROM HR_intervals as h, HR_threshold as hq
JOIN PED_intervals p ON h.time_interval = p.time_interval
ORDER BY h.time_interval;

SELECT * FROM analysis
INTO OUTFILE 'output_anomaly/Q2_anomaly_chdb.csv' FORMAT CSVWithNames;
"""

# USING CTE INSTEAD OF TEMP TABLES
query3 = f""" 
WITH HR_threshold AS (
    SELECT AVG(HR) + stddevSampStable(HR) AS threshold FROM hrm
),
hr_intervals AS (
    SELECT
        toStartOfMinute(ts) AS time_interval,
        AVG(HR) AS interval_HR
    FROM hrm
    GROUP BY toStartOfMinute(ts)
    ORDER BY toStartOfMinute(ts)
),
ped_intervals AS (
    SELECT
        toStartOfMinute(ts) AS time_interval,
        MAX(steps) - MIN(steps) AS interval_steps,
        MAX(calories) - MIN(calories) AS interval_calories
    FROM ped
    GROUP BY toStartOfMinute(ts)
    ORDER BY toStartOfMinute(ts)
)
SELECT
    h.time_interval as time_interval,
    h.interval_HR, p.interval_steps, p.interval_calories,
    CASE WHEN p.interval_steps < 10 AND h.interval_HR > q.threshold THEN 1 ELSE 0 END AS anomaly_flag
FROM hr_intervals h
JOIN ped_intervals p USING (time_interval)
CROSS JOIN HR_threshold q
INTO OUTFILE 'output_anomaly/Q3_anomaly_chdb.csv' FORMAT CSVWithNames;
"""

# SUBOPTIMALITIES: 
#   MULTIPLE TIMES BUCKET CALC
#   INSTEAD OF TIMESTAMPS WE USED STRINGS TO COMPARE
#   CALCULATING QUANTILE EACH TIME

query4 = f"""
SELECT 
    formatDateTime(toStartOfMinute(h.ts), '%Y-%m-%d %H:%i') AS time_interval,
    AVG(h.HR) AS interval_HR, 
    MAX(p.steps) - MIN(p.steps) AS interval_steps,
    MAX(p.calories) - MIN(p.calories) AS interval_calories,
    CASE WHEN (interval_steps < 10 AND interval_HR > (SELECT AVG(HR) + stddevSampStable(HR) FROM hrm)) THEN 1 ELSE 0 END AS anomaly_flag
FROM hrm AS h
JOIN ped AS p
    ON formatDateTime(toStartOfMinute(h.ts), '%Y-%m-%d %H:%i') = formatDateTime(toStartOfMinute(p.ts), '%Y-%m-%d %H:%i')
GROUP BY toStartOfMinute(h.ts)
ORDER BY toStartOfMinute(h.ts)
INTO OUTFILE 'output_anomaly/Q4_anomaly_chdb.csv' FORMAT CSVWithNames;
"""


# ==================================================================
db_path = "../db_ba30/ba30_data_chdb"
conn = chdb.connect(db_path)
conn.query("USE sensor;")

queries = {"q1": query1, "q3": query3, "q4": query4}

output_dir = "./output_anomaly"
os.makedirs(output_dir, exist_ok=True)

for name, q in queries.items():
    print(f"Executing {name} ...")
    output_path = os.path.join(output_dir, f"{name}_anomaly_chdb.csv")
    try:
        res = conn.query(q)
        print(res)
    except Exception as e:
        print(f"Failed to execute {name}: {e}")
        
conn.close()