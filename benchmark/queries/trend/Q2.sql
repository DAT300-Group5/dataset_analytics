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