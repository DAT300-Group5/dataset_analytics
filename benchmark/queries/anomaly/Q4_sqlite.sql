SELECT 
    (CAST(h.ts AS INTEGER)/60000)*60000 AS time_interval,
    AVG(h.HR) AS interval_HR, 
    MAX(p.steps) - MIN(p.steps) AS interval_steps,
    MAX(p.calories) - MIN(p.calories) AS interval_calories,

    CASE WHEN (((MAX(p.steps) - MIN(p.steps)) < 10) AND AVG(h.HR) > (SELECT AVG(HR) + AVG(HR*HR)-AVG(HR)*AVG(HR) AS threshold FROM hrm)) 
        THEN 1 ELSE 0 END AS anomaly_flag
FROM hrm AS h
JOIN ped AS p
    ON (CAST(h.ts AS INTEGER)/60000)*60000 = (CAST(p.ts AS INTEGER)/60000)*60000
GROUP BY time_interval
ORDER BY time_interval;