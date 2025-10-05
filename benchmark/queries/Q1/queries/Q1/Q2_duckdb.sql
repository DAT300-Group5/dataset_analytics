USE sensor;

WITH
hr_hour AS (
  SELECT
    deviceId,
    toStartOfMinute(ts) AS minute_dt,
    avg(HR) AS avg_hr
  FROM hrm
  GROUP BY deviceId, minute_dt
),