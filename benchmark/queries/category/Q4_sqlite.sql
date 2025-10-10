-- CTE
-- DESCRIPTION: 
-- - saves precomputed values
-- - each expression already joins on LIT so less rows for the final join

WITH LIT_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,
    AVG(ambient_light_intensity) AS avg_light_intensity
FROM lit
GROUP BY time_interval
HAVING AVG(ambient_light_intensity) > 100),

HR_intervals AS
(SELECT 
    l.time_interval,
    AVG(h.HR) as interval_HR
FROM hrm h JOIN LIT_intervals l ON (CAST(h.ts AS INTEGER) / 300000) * 300000 = l.time_interval
GROUP BY time_interval),

GYR_intervals AS
(SELECT 
    l.time_interval,
    AVG(x*x + y*y + z*z) AS gyr_magnitude
FROM gyr g JOIN LIT_intervals l ON (CAST(g.ts AS INTEGER) / 300000) * 300000 = l.time_interval
GROUP BY time_interval),

ACC_intervals AS
(SELECT 
    l.time_interval,
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc a JOIN LIT_intervals l ON (CAST(a.ts AS INTEGER) / 300000) * 300000 = l.time_interval
GROUP BY time_interval)

SELECT 
    h.time_interval, h.interval_HR, 
    a.acc_magnitude, g.gyr_magnitude,
    CASE 
        WHEN h.interval_HR < 80 OR (a.acc_magnitude < 2 and g.gyr_magnitude < 2) THEN 'sitting' 
        WHEN h.interval_HR < 110 OR (a.acc_magnitude < 10 and g.gyr_magnitude < 10) THEN 'light_activity'
        WHEN h.interval_HR >= 110 OR (a.acc_magnitude < 100 and g.gyr_magnitude < 100) THEN 'light_activity'
        ELSE 'misc'
        END AS type_of_activity
FROM HR_intervals as h 
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
ORDER BY h.time_interval;