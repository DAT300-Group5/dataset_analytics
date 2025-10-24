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
CROSS JOIN HR_threshold q;