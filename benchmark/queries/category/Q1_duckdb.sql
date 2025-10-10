-- === CTE
-- DESCRIPTION: 
-- - saves precomputed values
-- - filtering on daylight after the join

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
        WHEN h.interval_HR >= 110 OR (a.acc_magnitude < 100 AND g.gyr_magnitude < 100) THEN 'light_activity'
        ELSE 'misc' END AS type_of_activity
FROM HR_intervals as h 
JOIN LIT_intervals l ON h.time_interval = l.time_interval
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
WHERE l.is_light == 1
ORDER BY h.time_interval;