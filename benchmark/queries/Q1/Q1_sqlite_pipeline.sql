-- 1) Materialize per-minute aggregations and create a composite index (deviceId, minute_ms)
DROP TABLE IF EXISTS temp.hrm_minute;
CREATE TEMP TABLE hrm_minute AS
SELECT
  deviceId,
  (CAST(ts AS INTEGER)/60000)*60000 AS minute_ms,
  AVG(HR) AS avg_hr
FROM hrm
GROUP BY deviceId, minute_ms;
CREATE INDEX temp.hrm_minute_idx ON hrm_minute(deviceId, minute_ms);

DROP TABLE IF EXISTS temp.ppg_minute;
CREATE TEMP TABLE ppg_minute AS
SELECT
  deviceId,
  (CAST(ts AS INTEGER)/60000)*60000 AS minute_ms,
  AVG(ppg) AS avg_ppg
FROM ppg
GROUP BY deviceId, minute_ms;
CREATE INDEX temp.ppg_minute_idx ON ppg_minute(deviceId, minute_ms);

DROP TABLE IF EXISTS temp.acc_minute;
CREATE TEMP TABLE acc_minute AS
SELECT
  deviceId,
  (CAST(ts AS INTEGER)/60000)*60000 AS minute_ms,
  sqrt(AVG(x*x + y*y + z*z)) AS rms_acc
FROM acc
GROUP BY deviceId, minute_ms;
CREATE INDEX temp.acc_minute_idx ON acc_minute(deviceId, minute_ms);

DROP TABLE IF EXISTS temp.ped_minute;
CREATE TEMP TABLE ped_minute AS
SELECT
  deviceId,
  (CAST(ts AS INTEGER)/60000)*60000 AS minute_ms,
  SUM(steps) AS total_steps
FROM ped
GROUP BY deviceId, minute_ms;
CREATE INDEX temp.ped_minute_idx ON ped_minute(deviceId, minute_ms);

-- Median: rank within each minute partition, then aggregate to per-minute median
DROP TABLE IF EXISTS temp.lit_ranked;
CREATE TEMP TABLE lit_ranked AS
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
FROM lit;
CREATE INDEX temp.lit_ranked_idx ON lit_ranked(deviceId, minute_ms, rn);

DROP TABLE IF EXISTS temp.lit_minute;
CREATE TEMP TABLE lit_minute AS
SELECT
  deviceId,
  minute_ms,
  CASE
    WHEN cnt % 2 = 1 THEN MAX(CASE WHEN rn = (cnt + 1)/2 THEN ali END)
    ELSE (SUM(CASE WHEN rn = cnt/2 THEN ali END)
          +   SUM(CASE WHEN rn = cnt/2 + 1 THEN ali END)) / 2.0
  END AS median_light
FROM lit_ranked
GROUP BY deviceId, minute_ms, cnt;
CREATE INDEX temp.lit_minute_idx ON lit_minute(deviceId, minute_ms);

-- 2) Materialize the minutes key space and create an ordered index
DROP TABLE IF EXISTS temp.minutes;
CREATE TEMP TABLE minutes AS
SELECT deviceId, minute_ms FROM hrm_minute
UNION
SELECT deviceId, minute_ms FROM ppg_minute
UNION
SELECT deviceId, minute_ms FROM acc_minute
UNION
SELECT deviceId, minute_ms FROM ped_minute
UNION
SELECT deviceId, minute_ms FROM lit_minute;
CREATE INDEX temp.minutes_idx ON minutes(deviceId, minute_ms);

-- 3) Final statement (measured by ttfr_sqlite.py): stream results in index order
SELECT
  m.deviceId,
  strftime('%Y-%m-%dT%H:%M:%S', m.minute_ms/1000, 'unixepoch') || '+00:00' AS minute_ts,
  COALESCE(h.avg_hr,       0.0) AS avg_hr,
  COALESCE(p.avg_ppg,      0.0) AS avg_ppg,
  COALESCE(a.rms_acc,      0.0) AS rms_acc,
  COALESCE(d.total_steps,  0  ) AS total_steps,
  COALESCE(l.median_light, 0.0) AS median_light
FROM minutes AS m
LEFT JOIN hrm_minute AS h USING (deviceId, minute_ms)
LEFT JOIN ppg_minute AS p USING (deviceId, minute_ms)
LEFT JOIN acc_minute AS a USING (deviceId, minute_ms)
LEFT JOIN ped_minute AS d USING (deviceId, minute_ms)
LEFT JOIN lit_minute AS l USING (deviceId, minute_ms)
-- Key: keep ORDER BY consistent with minutes(deviceId, minute_ms) index to avoid full external sort
ORDER BY m.deviceId, m.minute_ms;
