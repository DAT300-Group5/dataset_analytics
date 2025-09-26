WITH
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
    sqrt(AVG(x*x + y*y + z*z)) AS rms_acc   -- true RMS
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
    quantileExact(0.5)(ambient_light_intensity) AS median_light
  FROM lit
  GROUP BY deviceId, minute_ts
),
minutes AS (
  SELECT deviceId, minute_ts FROM hrm_minute
  UNION DISTINCT
  SELECT deviceId, minute_ts FROM ppg_minute
  UNION DISTINCT
  SELECT deviceId, minute_ts FROM acc_minute
  UNION DISTINCT
  SELECT deviceId, minute_ts FROM ped_minute
  UNION DISTINCT
  SELECT deviceId, minute_ts FROM lit_minute
)
SELECT
  m.deviceId, m.minute_ts,
  h.avg_hr, p.avg_ppg, a.rms_acc, d.total_steps, l.median_light
FROM minutes m
LEFT JOIN hrm_minute h USING (deviceId, minute_ts)
LEFT JOIN ppg_minute p USING (deviceId, minute_ts)
LEFT JOIN acc_minute a USING (deviceId, minute_ts)
LEFT JOIN ped_minute d USING (deviceId, minute_ts)
LEFT JOIN lit_minute l USING (deviceId, minute_ts)
ORDER BY m.deviceId, m.minute_ts;
