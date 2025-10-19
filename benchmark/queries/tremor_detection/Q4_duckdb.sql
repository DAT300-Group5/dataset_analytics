
WITH
-- Divide dataset into 1 second windows for analysis
-- Calculate mean acc and mean gyr over each window
magnitude_windows_acc AS (
    SELECT 
        date_trunc('second', ts) AS time_window,
        sqrt(avg(x*x + y*y + z*z)) AS acc_mean_magnitude
    FROM acc
    GROUP BY 1
),
magnitude_windows_gyr AS (
    SELECT 
        date_trunc('second', ts) AS time_window,
        sqrt(avg(x*x + y*y + z*z)) AS gyr_mean_magnitude
    FROM gyr
    GROUP BY 1
),

magnitude_per_window AS (
    SELECT
        a.time_window,
        a.acc_mean_magnitude,
        g.gyr_mean_magnitude
    FROM magnitude_windows_acc a
    INNER JOIN magnitude_windows_gyr g
        USING (time_window)
),

-- Calculate if mean shows a change in direction
direction_change AS (
    SELECT
        time_window,
        acc_mean_magnitude,
        gyr_mean_magnitude,

        -- Difference between current and previous value (row)
        abs(acc_mean_magnitude - lag(acc_mean_magnitude) OVER w) AS acc_change,
        abs(gyr_mean_magnitude - lag(gyr_mean_magnitude) OVER w) AS gyr_change,

        CASE
            WHEN lag(acc_mean_magnitude, 2) OVER w IS NULL THEN 0
            WHEN (acc_mean_magnitude - lag(acc_mean_magnitude) OVER w)
                 * (lag(acc_mean_magnitude) OVER w - lag(acc_mean_magnitude, 2) OVER w) < 0
            THEN 1 ELSE 0
        END AS acc_direction_change,

        CASE
            WHEN lag(gyr_mean_magnitude, 2) OVER w IS NULL THEN 0
            WHEN (gyr_mean_magnitude - lag(gyr_mean_magnitude) OVER w)
                 * (lag(gyr_mean_magnitude) OVER w - lag(gyr_mean_magnitude, 2) OVER w) < 0
            THEN 1 ELSE 0
        END AS gyr_direction_change

    FROM magnitude_per_window
    WINDOW w AS (ORDER BY time_window)
),

-- get average acc and gyr change over neighboring rows
-- sum the amount of times a change in direction has occured
aggregate_data AS (
    SELECT
        time_window,
        acc_mean_magnitude,
        gyr_mean_magnitude,
        avg(acc_change) OVER w2 AS avg_acc_change,
        avg(gyr_change) OVER w2 AS avg_gyr_change,
        sum(acc_direction_change) OVER w2 AS acc_direction_change_rate,
        sum(gyr_direction_change) OVER w2 AS gyr_direction_change_rate
    FROM direction_change
    WINDOW w2 AS (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
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
            THEN 1 ELSE 0
        END AS tremor_detected
    FROM aggregate_data
)

--display number of tremors per day
SELECT
  date_trunc('day', time_window) AS date_time,
  sum(tremor_detected) as tremors_per_day
FROM detect_tremor
GROUP BY date_time
ORDER BY date_time;
