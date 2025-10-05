SELECT 
    time_bucket(INTERVAL '1m', h.ts) AS time_interval,
    AVG(h.HR) AS interval_HR, 
    MAX(p.steps) - MIN(p.steps) AS interval_steps,
    MAX(p.calories) - MIN(p.calories) AS interval_calories,
    CASE WHEN interval_steps < 10 AND interval_HR > (SELECT approx_quantile(HR, 0.8) FROM hrm) THEN 1 ELSE 0 END AS anomaly_flag
FROM hrm AS h
JOIN ped AS p
    ON strftime(time_bucket(INTERVAL '1m', h.ts), '%A, %-d %B %Y - %I:%M:%S %p') = strftime(time_bucket(INTERVAL '1m', p.ts), '%A, %-d %B %Y - %I:%M:%S %p')
GROUP BY time_bucket(INTERVAL '1m', h.ts)
ORDER BY time_bucket(INTERVAL '1m', h.ts);
