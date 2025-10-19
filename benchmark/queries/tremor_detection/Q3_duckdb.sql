
WITH
-- Divide dataset into 1 second windows for analysis
-- Calculate mean acc and mean gyr over each window
magnitude_windows_acc AS (
    SELECT 
    date_trunc('second', ts) AS time_window,
    sqrt(avg(x*x + y*y + z*z)) AS acc_mean_magnitude
    FROM acc
    GROUP BY time_window
    ORDER BY time_window
),

direction_change_acc AS (
    SELECT
    time_window,
    acc_mean_magnitude,

    -- Difference between current and previous value (row)
    abs(acc_mean_magnitude - lag(acc_mean_magnitude) OVER (ORDER BY time_window)) AS acc_change,

    -- check if there is a change in direction
    CASE
      WHEN lag(acc_mean_magnitude) OVER (ORDER BY time_window) IS NULL THEN 0
      WHEN (acc_mean_magnitude - lag(acc_mean_magnitude) OVER (ORDER BY time_window)) * (lag(acc_mean_magnitude) OVER (ORDER BY time_window) - lag(acc_mean_magnitude, 2) OVER (ORDER BY time_window)) < 0 THEN 1
      ELSE 0
    END AS acc_direction_change

  FROM magnitude_windows_acc
),

-- get average acc and gyr change over neighboring rows
-- sum the amount of times a change in direction has occured
aggregate_data_acc AS (
  SELECT
    time_window,
    acc_mean_magnitude,
    avg(acc_change)  OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_acc_change,
    sum(acc_direction_change)  OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS acc_direction_change_rate

  FROM direction_change_acc
),

magnitude_windows_gyr AS (
    SELECT 
    date_trunc('second', ts) AS time_window,
    sqrt(avg(x*x + y*y + z*z)) AS gyr_mean_magnitude
    FROM gyr
    GROUP BY time_window
    ORDER BY time_window
),

direction_change_gyr AS (
  SELECT
    time_window,
    gyr_mean_magnitude,

    -- Difference between current and previous value (row)
    abs(gyr_mean_magnitude - lag(gyr_mean_magnitude) OVER (ORDER BY time_window)) AS gyr_change,

    -- check if there is a change in direction
    CASE
      WHEN lag(gyr_mean_magnitude) OVER (ORDER BY time_window) IS NULL THEN 0
      WHEN (gyr_mean_magnitude - lag(gyr_mean_magnitude) OVER (ORDER BY time_window)) * (lag(gyr_mean_magnitude) OVER (ORDER BY time_window) - lag(gyr_mean_magnitude, 2) OVER (ORDER BY time_window)) < 0 THEN 1
      ELSE 0
    END AS gyr_direction_change

  FROM magnitude_windows_gyr
),

aggregate_data_gyr AS (
  SELECT
    time_window,
    gyr_mean_magnitude,
    avg(gyr_change) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_gyr_change,
    sum(gyr_direction_change) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS gyr_direction_change_rate
  FROM direction_change_gyr
),

join_data AS (
    SELECT
    a.time_window,
    acc_mean_magnitude,
    gyr_mean_magnitude,
    avg_acc_change,
    avg_gyr_change,
    acc_direction_change_rate,
    gyr_direction_change_rate

  FROM aggregate_data_acc a
  JOIN aggregate_data_gyr g  ON a.time_window = g.time_window
  ORDER BY a.time_window
),

-- check if the avverage acc change and average gyr change are over threshold
-- and average direction change within threshold
detect_tremor AS (
  SELECT
    time_window,
    acc_mean_magnitude,
    gyr_mean_magnitude,
    avg_acc_change,
    avg_gyr_change,
    acc_direction_change_rate,
    gyr_direction_change_rate,

    CASE
      WHEN (avg_acc_change > 3 OR avg_gyr_change > 15)
        AND (acc_direction_change_rate BETWEEN 2 AND 20 OR gyr_direction_change_rate BETWEEN 2 AND 20)
      THEN 1 
      ELSE 0
    END AS tremor_detected
  FROM join_data
)
 
--display number of tremors per day
SELECT
  date_trunc('day', time_window) AS date_time,
  sum(tremor_detected) as tremors_per_day
FROM detect_tremor
GROUP BY date_time
ORDER BY date_time;
