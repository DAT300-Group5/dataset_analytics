WITH
hrm_minute AS (
  SELECT
    deviceId,
    (CAST(ts AS INTEGER)/60000)*60000 AS minute_ms,
    AVG(HR) AS avg_hr
  FROM hrm
  GROUP BY deviceId, minute_ms
),
ppg_minute AS (
  SELECT
    deviceId,
    (CAST(ts AS INTEGER)/60000)*60000 AS minute_ms,
    AVG(ppg) AS avg_ppg
  FROM ppg
  GROUP BY deviceId, minute_ms
),
acc_minute AS (
  SELECT
    deviceId,
    (CAST(ts AS INTEGER)/60000)*60000 AS minute_ms,
    sqrt(AVG(x*x + y*y + z*z)) AS rms_acc
  FROM acc
  GROUP BY deviceId, minute_ms
),
ped_minute AS (
  SELECT
    deviceId,
    (CAST(ts AS INTEGER)/60000)*60000 AS minute_ms,
    SUM(steps) AS steps_sum
  FROM ped
  GROUP BY deviceId, minute_ms
),
lit_ranked AS (
  SELECT
    deviceId,
    (CAST(ts AS INTEGER)/60000)*60000 AS minute_ms,
    ambient_light_intensity AS ali,
    ROW_NUMBER() OVER (
      PARTITION BY deviceId, (CAST(ts AS INTEGER)/60000)*60000
      ORDER BY ambient_light_intensity
    ) AS rn,
    COUNT(*) OVER (
      PARTITION BY deviceId, (CAST(ts AS INTEGER)/60000)*60000
    ) AS cnt
  FROM lit
),
lit_minute AS (
  SELECT
    deviceId,
    minute_ms,
    CASE
      WHEN cnt % 2 = 1 THEN MAX(CASE WHEN rn = (cnt + 1)/2 THEN ali END)
      ELSE (SUM(CASE WHEN rn = cnt/2 THEN ali END)
            + SUM(CASE WHEN rn = cnt/2 + 1 THEN ali END)) / 2.0
    END AS median_light
  FROM lit_ranked
  GROUP BY deviceId, minute_ms
),
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
  h.avg_hr,
  p.avg_ppg,
  a.rms_acc,
  d.steps_sum,
  l.median_light
FROM minutes AS m
LEFT JOIN hrm_minute AS h USING (deviceId, minute_ms)
LEFT JOIN ppg_minute AS p USING (deviceId, minute_ms)
LEFT JOIN acc_minute AS a USING (deviceId, minute_ms)
LEFT JOIN ped_minute AS d USING (deviceId, minute_ms)
LEFT JOIN lit_minute AS l USING (deviceId, minute_ms)
ORDER BY m.deviceId, m.minute_ms;
;
