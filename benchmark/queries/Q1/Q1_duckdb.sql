SET timezone='UTC';

-- Enable JSON profiling for DuckDB
PRAGMA enable_profiling='json';

SET profiling_output='results/profiling_query_1.json';

SELECT * FROM hrm LIMIT 5;

SET profiling_output='results/profiling_query_2.json';

WITH
hrm_minute AS (
  SELECT
    deviceId,
    date_trunc('minute', ts) AS minute_dt,
    AVG(HR) AS avg_hr
  FROM hrm
  GROUP BY 1,2
),
ppg_minute AS (
  SELECT
    deviceId,
    date_trunc('minute', ts) AS minute_dt,
    AVG(ppg) AS avg_ppg
  FROM ppg
  GROUP BY 1,2
),
acc_minute AS (
  SELECT
    deviceId,
    date_trunc('minute', ts) AS minute_dt,
    sqrt(AVG(x*x + y*y + z*z)) AS rms_acc
  FROM acc
  GROUP BY 1,2
),
ped_minute AS (
  SELECT
    deviceId,
    date_trunc('minute', ts) AS minute_dt,
    SUM(steps) AS total_steps
  FROM ped
  GROUP BY 1,2
),
lit_minute AS (
  SELECT
    deviceId,
    date_trunc('minute', ts) AS minute_dt,
    median(ambient_light_intensity) AS median_light
  FROM lit
  GROUP BY 1,2
),
minutes AS (
  SELECT deviceId, minute_dt FROM hrm_minute
  UNION
  SELECT deviceId, minute_dt FROM ppg_minute
  UNION
  SELECT deviceId, minute_dt FROM acc_minute
  UNION
  SELECT deviceId, minute_dt FROM ped_minute
  UNION
  SELECT deviceId, minute_dt FROM lit_minute
)
SELECT
  m.deviceId,
  strftime(m.minute_dt, '%Y-%m-%dT%H:%M:%S+00:00') AS minute_ts,
  COALESCE(h.avg_hr,       0.0) AS avg_hr,
  COALESCE(p.avg_ppg,      0.0) AS avg_ppg,
  COALESCE(a.rms_acc,      0.0) AS rms_acc,
  COALESCE(d.total_steps,  0  ) AS total_steps,
  COALESCE(l.median_light, 0.0) AS median_light
FROM minutes m
LEFT JOIN hrm_minute h USING (deviceId, minute_dt)
LEFT JOIN ppg_minute p USING (deviceId, minute_dt)
LEFT JOIN acc_minute a USING (deviceId, minute_dt)
LEFT JOIN ped_minute d USING (deviceId, minute_dt)
LEFT JOIN lit_minute l USING (deviceId, minute_dt)
ORDER BY m.deviceId, m.minute_dt;
