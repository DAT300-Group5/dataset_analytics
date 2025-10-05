
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import duckdb

print(duckdb.__version__)

db_path = "db_ba30/ba30_data.duckdb"

# ==============================================
# TREND ANALYSIS ===============================
# FITNESS INDEX TREND ANALYSIS 
# ==============================================

# TEMP TABLES
query1 = f"""
DROP TABLE IF EXISTS PED_intervals;
DROP TABLE IF EXISTS HR_intervals;
DROP TABLE IF EXISTS fitness_index_intervals;
DROP TABLE IF EXISTS analysis;

CREATE TEMP TABLE HR_intervals AS
SELECT 
    time_bucket(INTERVAL '30m', ts) AS time_interval,
    AVG(HR) as interval_HR
FROM hrm
GROUP BY time_interval
ORDER BY time_interval;

SELECT * FROM HR_intervals;

CREATE TEMP TABLE PED_intervals AS
SELECT 
    time_bucket(INTERVAL '30m', ts) AS time_interval,
    MAX(steps) - MIN(steps) AS steps_interval,
    MAX(calories) - MIN(calories) AS cal_interval
FROM ped
GROUP BY time_interval
ORDER BY time_interval;

CREATE TEMP TABLE fitness_index_intervals AS
SELECT 
    h.time_interval, 
    p.steps_interval / h.interval_HR AS fitness_index
FROM HR_intervals as h 
JOIN PED_intervals p ON h.time_interval = p.time_interval
WHERE h.interval_HR>0
ORDER BY h.time_interval;

SELECT
    REGR_SLOPE(fitness_index, EXTRACT(epoch FROM time_interval)) AS slope_fi,
    REGR_INTERCEPT(fitness_index, EXTRACT(epoch FROM time_interval)) AS intercept_fi
FROM fitness_index_intervals;
"""

# IF WE WANTED TO CALC DERIV INSTEAD 
"""
SELECT 
    time_interval,
    fitness_index - LAG(fitness_index) OVER (ORDER BY time_interval) AS delta_fi
FROM fitness_index_intervals
"""

# CTE
query2 = f"""

WITH HR_intervals AS
(SELECT 
    time_bucket(INTERVAL '30m', ts) AS time_interval,
    AVG(HR) as interval_HR
FROM hrm
GROUP BY time_interval
ORDER BY time_interval),

PED_intervals AS
(SELECT 
    time_bucket(INTERVAL '30m', ts) AS time_interval,
    MAX(steps) - MIN(steps) AS steps_interval,
    MAX(calories) - MIN(calories) AS cal_interval
FROM ped
GROUP BY time_interval
ORDER BY time_interval),

fitness_index_intervals AS
(SELECT 
    h.time_interval, 
    p.steps_interval / h.interval_HR AS fitness_index
FROM HR_intervals as h 
JOIN PED_intervals p ON h.time_interval = p.time_interval
WHERE h.interval_HR>0
ORDER BY h.time_interval)

SELECT
    REGR_SLOPE(fitness_index, EXTRACT(epoch FROM time_interval)) AS slope_fi,
    REGR_INTERCEPT(fitness_index, EXTRACT(epoch FROM time_interval)) AS intercept_fi
FROM fitness_index_intervals;
"""

# ONE QUERY ONLY 
# ORDER BY IN SUBQUERIES
query3 = f"""
SELECT
    REGR_SLOPE(fitness_index, EXTRACT(epoch FROM time_interval)) AS slope_fi,
    REGR_INTERCEPT(fitness_index, EXTRACT(epoch FROM time_interval)) AS intercept_fi
FROM (
    SELECT
        h.time_interval,
        h.interval_HR,
        p.steps_interval,
        p.steps_interval / h.interval_HR AS fitness_index
    FROM (SELECT time_bucket(INTERVAL '30m', ts) AS time_interval, AVG(HR) as interval_HR FROM hrm GROUP BY time_interval ORDER BY time_interval) AS h
    JOIN 
        (SELECT time_bucket(INTERVAL '30m', ts) AS time_interval, MAX(steps) - MIN(steps) AS steps_interval FROM ped GROUP BY time_interval ORDER BY time_interval) AS p
        ON h.time_interval = p.time_interval
        WHERE h.interval_HR > 0
) AS aggregated_intervals;
"""

# ONE QUERY ONLY 
# WITHOUT ORDER BY IN SUBQUERIES
query4 = f"""
SELECT
    REGR_SLOPE(fitness_index, EXTRACT(epoch FROM time_interval)) AS slope_fi,
    REGR_INTERCEPT(fitness_index, EXTRACT(epoch FROM time_interval)) AS intercept_fi
FROM (
    SELECT
        h.time_interval,
        h.interval_HR,
        p.steps_interval,
        p.steps_interval / h.interval_HR AS fitness_index
    FROM (SELECT time_bucket(INTERVAL '30m', ts) AS time_interval, AVG(HR) as interval_HR FROM hrm GROUP BY time_interval) AS h
    JOIN 
        (SELECT time_bucket(INTERVAL '30m', ts) AS time_interval, MAX(steps) - MIN(steps) AS steps_interval FROM ped GROUP BY time_interval) AS p
        ON h.time_interval = p.time_interval
        WHERE h.interval_HR > 0
) AS aggregated_intervals;
"""

# UNNECESSARY GROUP BYs
query5 = f"""
SELECT
    REGR_SLOPE(fitness_index, EXTRACT(epoch FROM time_interval)) AS slope_fi,
    REGR_INTERCEPT(fitness_index, EXTRACT(epoch FROM time_interval)) AS intercept_fi
FROM (
    SELECT
        h.deviceId,
        h.time_interval,
        h.interval_HR,
        p.steps_interval,
        p.steps_interval / h.interval_HR AS fitness_index
    FROM (SELECT deviceId, time_bucket(INTERVAL '30m', ts) AS time_interval, AVG(HR) as interval_HR FROM hrm GROUP BY time_interval, deviceId) AS h
    JOIN 
        (SELECT deviceId, time_bucket(INTERVAL '30m', ts) AS time_interval, MAX(steps) - MIN(steps) AS steps_interval FROM ped GROUP BY time_interval, deviceId) AS p
        ON h.time_interval = p.time_interval
        WHERE h.interval_HR > 0
) AS aggregated_intervals;
"""

con = duckdb.connect(database=db_path)

for query in [query1, query2, query3, query4, query5]:
    cols, rows = None, []
    result = con.execute(query).fetchall()
    print(result[:1])
    cur = con.cursor()
    if cur.description:
        cols = [d[0] for d in cur.description]

con.close()
