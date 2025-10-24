USE sensor;

SELECT 
    toStartOfMinute(h.ts) AS time_interval,
    AVG(h.HR) AS interval_HR, 
    MAX(p.steps) - MIN(p.steps) AS interval_steps,
    MAX(p.calories) - MIN(p.calories) AS interval_calories,
    CASE WHEN (interval_steps < 10 AND interval_HR > (SELECT AVG(HR) + stddevSampStable(HR) FROM hrm)) THEN 1 ELSE 0 END AS anomaly_flag
FROM hrm AS h
JOIN ped AS p
    ON toStartOfMinute(h.ts) = toStartOfMinute(p.ts)
GROUP BY toStartOfMinute(h.ts)
ORDER BY toStartOfMinute(h.ts);