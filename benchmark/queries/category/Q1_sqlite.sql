WITH HR_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(HR) AS interval_HR
FROM hrm
WHERE ts BETWEEN 1615676400000 AND 1616367599000
GROUP BY time_interval
),

GYR_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(x*x + y*y + z*z) AS gyr_magnitude
FROM gyr
WHERE ts BETWEEN 1615676400000 AND 1616367599000
GROUP BY time_interval
),

ACC_intervals AS (
SELECT 
    (CAST(ts AS INTEGER) / 300000) * 300000 AS time_interval,  
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc
WHERE ts BETWEEN 1615676400000 AND 1616367599000
GROUP BY time_interval
)

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
FROM HR_intervals h
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
ORDER BY h.time_interval;
