USE sensor;

WITH
-- 1) Per-minute aggregates
hrm_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(toDateTime(ts/1000, 'UTC')) AS minute_ts,
    AVG(HR) AS avg_hr
  FROM hrm
  GROUP BY deviceId, minute_ts
),
ppg_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(toDateTime(ts/1000, 'UTC')) AS minute_ts,
    AVG(ppg) AS avg_ppg
  FROM ppg
  GROUP BY deviceId, minute_ts
),
acc_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(toDateTime(ts/1000, 'UTC')) AS minute_ts,
    sqrt(AVG(x*x + y*y + z*z)) AS rms_acc
  FROM acc
  GROUP BY deviceId, minute_ts
),
ped_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(toDateTime(ts/1000, 'UTC')) AS minute_ts,
    SUM(steps) AS total_steps
  FROM ped
  GROUP BY deviceId, minute_ts
),
lit_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(toDateTime(ts/1000, 'UTC')) AS minute_ts,
    quantileExactInclusive(0.5)(ambient_light_intensity) AS median_light
  FROM lit
  GROUP BY deviceId, minute_ts
),

-- 2) Build minutes from the aggregated CTEs
minutes_raw AS (
  SELECT deviceId, minute_ts FROM hrm_minute
  UNION ALL
  SELECT deviceId, minute_ts FROM ppg_minute
  UNION ALL
  SELECT deviceId, minute_ts FROM acc_minute
  UNION ALL
  SELECT deviceId, minute_ts FROM ped_minute
  UNION ALL
  SELECT deviceId, minute_ts FROM lit_minute
),
minutes AS (
  SELECT DISTINCT deviceId, minute_ts FROM minutes_raw
)

-- 3) Stitch together
SELECT
  m.deviceId,
  m.minute_ts,
  h.avg_hr,
  p.avg_ppg,
  a.rms_acc,
  d.total_steps,
  l.median_light
FROM minutes AS m
LEFT JOIN hrm_minute AS h USING (deviceId, minute_ts)
LEFT JOIN ppg_minute AS p USING (deviceId, minute_ts)
LEFT JOIN acc_minute AS a USING (deviceId, minute_ts)
LEFT JOIN ped_minute AS d USING (deviceId, minute_ts)
LEFT JOIN lit_minute AS l USING (deviceId, minute_ts)
ORDER BY m.deviceId, m.minute_ts;