WITH
minutes AS (
  SELECT deviceId, toStartOfMinute(toDateTime(ts/1000, 'UTC')) AS minute_ts FROM hrm
  UNION DISTINCT
  SELECT deviceId, toStartOfMinute(toDateTime(ts/1000, 'UTC')) FROM ppg
  UNION DISTINCT
  SELECT deviceId, toStartOfMinute(toDateTime(ts/1000, 'UTC')) FROM acc
  UNION DISTINCT
  SELECT deviceId, toStartOfMinute(toDateTime(ts/1000, 'UTC')) FROM ped
  UNION DISTINCT
  SELECT deviceId, toStartOfMinute(toDateTime(ts/1000, 'UTC')) FROM lit
),

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
    quantileExactInclusive(0.5)(ambient_light_intensity) AS median_light
  FROM lit
  GROUP BY deviceId, minute_ts
)

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
