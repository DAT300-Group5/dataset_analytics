USE sensor;

WITH
hrm_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(ts) AS minute_dt,
    avg(HR) AS avg_hr
  FROM hrm
  GROUP BY deviceId, minute_dt
),
ppg_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(ts) AS minute_dt,
    avg(ppg) AS avg_ppg
  FROM ppg
  GROUP BY deviceId, minute_dt
),
acc_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(ts) AS minute_dt,
    sqrt(avg(x*x + y*y + z*z)) AS rms_acc
  FROM acc
  GROUP BY deviceId, minute_dt
),
ped_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(ts) AS minute_dt,
    sum(steps) AS total_steps
  FROM ped
  GROUP BY deviceId, minute_dt
),
lit_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(ts) AS minute_dt,
    quantileExactInclusive(0.5)(ambient_light_intensity) AS median_light
  FROM lit
  GROUP BY deviceId, minute_dt
),
minutes AS (
  SELECT deviceId, minute_dt FROM hrm_minute
  UNION DISTINCT
  SELECT deviceId, minute_dt FROM ppg_minute
  UNION DISTINCT
  SELECT deviceId, minute_dt FROM acc_minute
  UNION DISTINCT
  SELECT deviceId, minute_dt FROM ped_minute
  UNION DISTINCT
  SELECT deviceId, minute_dt FROM lit_minute
)
SELECT
  m.deviceId AS deviceId,
  formatDateTime(m.minute_dt, '%Y-%m-%dT%H:%i:%S+00:00') AS minute_ts,
  COALESCE(h.avg_hr,       0.0) AS avg_hr,
  COALESCE(p.avg_ppg,      0.0) AS avg_ppg,
  COALESCE(a.rms_acc,      0.0) AS rms_acc,
  COALESCE(d.total_steps,  0  ) AS total_steps,
  COALESCE(l.median_light, 0.0) AS median_light
FROM minutes AS m
LEFT JOIN hrm_minute AS h USING (deviceId, minute_dt)
LEFT JOIN ppg_minute AS p USING (deviceId, minute_dt)
LEFT JOIN acc_minute AS a USING (deviceId, minute_dt)
LEFT JOIN ped_minute AS d USING (deviceId, minute_dt)
LEFT JOIN lit_minute AS l USING (deviceId, minute_dt)
ORDER BY m.deviceId, m.minute_dt;
