
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import duckdb
import os

print("DuckDB version:", duckdb.__version__, "\n")

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
    time_bucket(INTERVAL '1m', h.ts) AS time_interval,
    AVG(h.HR) AS interval_HR, 
    MAX(p.steps) - MIN(p.steps) AS interval_steps,
    MAX(p.calories) - MIN(p.calories) AS interval_calories,
    CASE WHEN (interval_steps < 10 AND interval_HR > (SELECT AVG(HR) + STDDEV(HR) FROM hrm)) THEN 1 ELSE 0 END AS anomaly_flag
FROM hrm AS h
JOIN ped AS p
    ON time_bucket(INTERVAL '1m', h.ts) = time_bucket(INTERVAL '1m', p.ts)
GROUP BY time_bucket(INTERVAL '1m', h.ts)
ORDER BY time_bucket(INTERVAL '1m', h.ts);
"""

# SUBOPTIMAL: multiple temporary tables
# OPTIMAL:  we don't calculate time buckets multiple times (when joining) 
#           we don't calculate quantile multiple times (when filtering rows)
query2 = f"""
DROP TABLE IF EXISTS PED_intervals;
DROP TABLE IF EXISTS HR_intervals;
DROP TABLE IF EXISTS analysis;
DROP TABLE IF EXISTS HR_threshold;

CREATE TEMP TABLE HR_intervals AS
SELECT 
    time_bucket(INTERVAL '1m', ts) AS time_interval,
    AVG(HR) as interval_HR
FROM hrm
GROUP BY time_interval
ORDER BY time_interval;

CREATE TEMP TABLE HR_threshold AS
SELECT AVG(HR) + STDDEV(HR) AS threshold FROM hrm;

CREATE TEMP TABLE PED_intervals AS
SELECT 
    time_bucket(INTERVAL '1m', ts) AS time_interval,
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

SELECT * FROM analysis;
"""

# USING CTE INSTEAD OF TEMP TABLES
query3 = f""" 
WITH HR_threshold AS (
    SELECT AVG(HR) + STDDEV(HR) AS threshold FROM hrm
),
hr_intervals AS (
    SELECT
        time_bucket(INTERVAL '1m', ts) AS time_interval,
        AVG(HR) AS interval_HR
    FROM hrm
    GROUP BY time_bucket(INTERVAL '1m', ts)
    ORDER BY time_bucket(INTERVAL '1m', ts)
),
ped_intervals AS (
    SELECT
        time_bucket(INTERVAL '1m', ts) AS time_interval,
        MAX(steps) - MIN(steps) AS interval_steps,
        MAX(calories) - MIN(calories) AS interval_calories
    FROM ped
    GROUP BY time_bucket(INTERVAL '1m', ts)
    ORDER BY time_bucket(INTERVAL '1m', ts)
)
SELECT
    h.time_interval,
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
    time_bucket(INTERVAL '1m', h.ts) AS time_interval,
    AVG(h.HR) AS interval_HR, 
    MAX(p.steps) - MIN(p.steps) AS interval_steps,
    MAX(p.calories) - MIN(p.calories) AS interval_calories,
    CASE WHEN interval_steps < 10 AND interval_HR > (SELECT AVG(HR) + STDDEV(HR) FROM hrm) THEN 1 ELSE 0 END AS anomaly_flag
FROM hrm AS h
JOIN ped AS p
    ON strftime(time_bucket(INTERVAL '1m', h.ts), '%A, %-d %B %Y - %I:%M:%S %p') = strftime(time_bucket(INTERVAL '1m', p.ts), '%A, %-d %B %Y - %I:%M:%S %p')
GROUP BY time_bucket(INTERVAL '1m', h.ts)
ORDER BY time_bucket(INTERVAL '1m', h.ts);
"""

# =====================================================================
db_path = "../db_ba30/ba30_data.duckdb"
con = duckdb.connect(database=db_path)
cols, rows = None, []
queries = {"q1": query1,"q2": query2, "q3":query3, "q4":query4}

out_dir = "output_anomaly"
os.makedirs(out_dir, exist_ok=True)

for q, stmt in queries.items():
    print("executing:", q)
    df = con.execute(f"{stmt}").fetch_df()
    df.to_csv(f"{out_dir}/{q.upper()}_anomaly_duckdb.csv", index=False)

con.close()
