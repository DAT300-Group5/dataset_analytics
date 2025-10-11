--- QUERY DESCRIPTION: SINGLE QUERY
-- EFFECICIENCY: 
-- - not precomputed tables/expressions, needs to calculate time buckets 
-- 


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