WITH
-- Per-minute aggregates for each table (ts in ms → seconds before to_timestamp)
hrm_minute AS (
  SELECT deviceId,
         date_trunc('minute', to_timestamp(ts/1000)) AS minute_ts,
         AVG(HR) AS avg_hr
  FROM hrm
  GROUP BY 1,2
),
ppg_minute AS (
  SELECT deviceId,
         date_trunc('minute', to_timestamp(ts/1000)) AS minute_ts,
         AVG(ppg) AS avg_ppg
  FROM ppg
  GROUP BY 1,2
),
acc_minute AS (
  SELECT deviceId,
         date_trunc('minute', to_timestamp(ts/1000)) AS minute_ts,
         sqrt(AVG(x*x + y*y + z*z)) AS rms_acc       -- true RMS (aligned with SQLite)
  FROM acc
  GROUP BY 1,2
),
ped_minute AS (
  SELECT deviceId,
         date_trunc('minute', to_timestamp(ts/1000)) AS minute_ts,
         SUM(steps) AS total_steps
  FROM ped
  GROUP BY 1,2
),
lit_minute AS (
  SELECT deviceId,
         date_trunc('minute', to_timestamp(ts/1000)) AS minute_ts,
         median(ambient_light_intensity) AS median_light
  FROM lit
  GROUP BY 1,2
),
-- Superset of (deviceId, minute)
minutes AS (
  SELECT deviceId, minute_ts FROM hrm_minute
  UNION
  SELECT deviceId, minute_ts FROM ppg_minute
  UNION
  SELECT deviceId, minute_ts FROM acc_minute
  UNION
  SELECT deviceId, minute_ts FROM ped_minute
  UNION
  SELECT deviceId, minute_ts FROM lit_minute
)
SELECT
  m.deviceId,
  m.minute_ts,
  h.avg_hr, p.avg_ppg, a.rms_acc, d.total_steps, l.median_light
FROM minutes m
LEFT JOIN hrm_minute h USING (deviceId, minute_ts)
LEFT JOIN ppg_minute p USING (deviceId, minute_ts)
LEFT JOIN acc_minute a USING (deviceId, minute_ts)
LEFT JOIN ped_minute d USING (deviceId, minute_ts)
LEFT JOIN lit_minute l USING (deviceId, minute_ts)
ORDER BY m.deviceId, m.minute_ts;
