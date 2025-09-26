WITH
-- 1) Per-minute aggregates on each table
hrm_minute AS (
  SELECT deviceId,
         (ts/60000)*60000 AS minute_ms,
         AVG(HR) AS avg_hr
  FROM hrm
  GROUP BY 1,2
),
ppg_minute AS (
  SELECT deviceId,
         (ts/60000)*60000 AS minute_ms,
         AVG(ppg) AS avg_ppg
  FROM ppg
  GROUP BY 1,2
),
acc_minute AS (
  SELECT deviceId,
         (ts/60000)*60000 AS minute_ms,
         -- True RMS of acceleration magnitude (change back if you intended avg|mag|)
         sqrt(AVG(x*x + y*y + z*z)) AS rms_acc
  FROM acc
  GROUP BY 1,2
),
ped_minute AS (
  SELECT deviceId,
         (ts/60000)*60000 AS minute_ms,
         SUM(steps) AS total_steps
  FROM ped
  GROUP BY 1,2
),
-- 2) Median for light per (device, minute) via window (kept, but index will help)
lit_ranked AS (
  SELECT
    deviceId,
    (ts/60000)*60000 AS minute_ms,
    ambient_light_intensity AS ali,
    ROW_NUMBER() OVER (
      PARTITION BY deviceId, (ts/60000)*60000
      ORDER BY ambient_light_intensity
    ) AS rn,
    COUNT(*) OVER (
      PARTITION BY deviceId, (ts/60000)*60000
    ) AS cnt
  FROM lit
),
lit_minute AS (
  SELECT deviceId, minute_ms,
         AVG(ali) AS median_light
  FROM lit_ranked
  WHERE rn IN ( (cnt+1)/2, (cnt+2)/2 )
  GROUP BY 1,2
),
-- 3) Build the superset of (deviceId, minute) from the already-aggregated CTEs
minutes AS (
  SELECT deviceId, minute_ms FROM hrm_minute
  UNION
  SELECT deviceId, minute_ms FROM ppg_minute
  UNION
  SELECT deviceId, minute_ms FROM acc_minute
  UNION
  SELECT deviceId, minute_ms FROM ped_minute
  UNION
  SELECT deviceId, minute_ms FROM lit_minute
)
SELECT
  m.deviceId,
  datetime(m.minute_ms/1000, 'unixepoch') AS minute_ts,
  h.avg_hr, p.avg_ppg, a.rms_acc, d.total_steps, l.median_light
FROM minutes m
LEFT JOIN hrm_minute h USING (deviceId, minute_ms)
LEFT JOIN ppg_minute p USING (deviceId, minute_ms)
LEFT JOIN acc_minute a USING (deviceId, minute_ms)
LEFT JOIN ped_minute d USING (deviceId, minute_ms)
LEFT JOIN lit_minute l USING (deviceId, minute_ms)
ORDER BY m.deviceId, m.minute_ms;
