

WITH
magnitude_per_window AS (
  SELECT
    (CAST(a.ts AS INTEGER)/1000)*1000 AS time_window,
    sqrt(avg(a.x*a.x + a.y*a.y + a.z*a.z)) AS acc_mean_magnitude,
    sqrt(avg(g.x*g.x + g.y*g.y + g.z*g.z)) AS gyr_mean_magnitude
  FROM acc a
  JOIN gyr g ON (CAST(a.ts AS INTEGER)/1000)*1000 = (CAST(g.ts AS INTEGER)/1000)*1000
  GROUP BY time_window
  ORDER BY time_window
),

features AS (
  SELECT
    time_window,
    acc_mean_magnitude,
    gyr_mean_magnitude,

    ABS(acc_mean_magnitude - lag(acc_mean_magnitude) OVER (ORDER BY time_window)) AS acc_change,
    ABS(gyr_mean_magnitude - lag(gyr_mean_magnitude) OVER (ORDER BY time_window)) AS gyr_change,

    CASE
      WHEN lag(acc_mean_magnitude) OVER (ORDER BY time_window) IS NULL THEN 0
      WHEN (acc_mean_magnitude - lag(acc_mean_magnitude) OVER (ORDER BY time_window)) *
           (lag(acc_mean_magnitude) OVER (ORDER BY time_window) -
            lag(acc_mean_magnitude, 2) OVER (ORDER BY time_window)) < 0 THEN 1
      ELSE 0
    END AS acc_zero_cross,

    CASE
      WHEN lag(gyr_mean_magnitude) OVER (ORDER BY time_window) IS NULL THEN 0
      WHEN (gyr_mean_magnitude - lag(gyr_mean_magnitude) OVER (ORDER BY time_window)) *
           (lag(gyr_mean_magnitude) OVER (ORDER BY time_window) -
            lag(gyr_mean_magnitude, 2) OVER (ORDER BY time_window)) < 0 THEN 1
      ELSE 0
    END AS gyr_zero_cross

  FROM magnitude_per_window
),

aggregate_data AS (
  SELECT
    time_window,
    acc_mean_magnitude,
    gyr_mean_magnitude,
    avg(acc_change) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_acc_change,
    avg(gyr_change) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_gyr_change,
    sum(acc_zero_cross) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS acc_zero_rate,
    sum(gyr_zero_cross) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS gyr_zero_rate
  FROM features
),

detect_tremor AS (
  SELECT
    time_window,
    acc_mean_magnitude,
    gyr_mean_magnitude,
    avg_acc_change,
    avg_gyr_change,
    acc_zero_rate,
    gyr_zero_rate,
    CASE
      WHEN (avg_acc_change > 0.05 OR avg_gyr_change > 0.02)
        AND (acc_zero_rate BETWEEN 0.3 AND 1.2 OR gyr_zero_rate BETWEEN 0.3 AND 1.2)
      THEN 1
      ELSE 0
    END AS tremor_detected
  FROM aggregate_data
)

SELECT
  --time_window,
  round(acc_mean_magnitude, 4) AS acc_mean_magnitude,
  round(gyr_mean_magnitude, 4) AS gyr_mean_magnitude,
  round(avg_acc_change, 4) AS avg_acc_change,
  round(avg_gyr_change, 4) AS avg_gyr_change,
  tremor_detected
FROM detect_tremor
ORDER BY time_window;

