WITH HR_threshold AS (
    SELECT AVG(HR) + exp(0.5 * ln(AVG(HR*HR)-AVG(HR)*AVG(HR))) AS threshold FROM hrm 
),
hr_intervals AS (
    SELECT
        (CAST(ts AS INTEGER)/60000)*60000 AS time_interval,
        AVG(HR) AS interval_HR
    FROM hrm
    GROUP BY time_interval
    ORDER BY time_interval
),
ped_intervals AS (
    SELECT
        (CAST(ts AS INTEGER)/60000)*60000 AS time_interval,
        MAX(steps) - MIN(steps) AS interval_steps,
        MAX(calories) - MIN(calories) AS interval_calories
    FROM ped
    GROUP BY time_interval
    ORDER BY time_interval
)
SELECT
    h.time_interval as time_interval,
    h.interval_HR, p.interval_steps, p.interval_calories,
    CASE WHEN p.interval_steps < 10 AND h.interval_HR > q.threshold THEN 1 ELSE 0 END AS anomaly_flag
FROM hr_intervals h
JOIN ped_intervals p USING (time_interval)
CROSS JOIN HR_threshold q;