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
