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
SELECT AVG(HR) + AVG(HR*HR)-AVG(HR)*AVG(HR) AS threshold FROM hrm;

CREATE TEMP TABLE PED_intervals AS
SELECT 
    (CAST(ts AS INTEGER)/60000)*60000 AS time_interval,
    MAX(steps) - MIN(steps) AS steps_interval,
    MAX(calories) - MIN(calories) AS cal_interval
FROM ped
GROUP BY time_interval
ORDER BY time_interval;

SELECT 
    h.time_interval, h.interval_HR, 
    p.steps_interval, p.cal_interval,
    CASE WHEN (p.steps_interval < 10 AND h.interval_HR > q.threshold) THEN 1 ELSE 0 END AS anomaly_flag
FROM HR_intervals as h, HR_threshold as q
JOIN PED_intervals p ON h.time_interval = p.time_interval
ORDER BY h.time_interval;