USE sensor;

WITH
-- Divide dataset into 1 second windows for analysis
-- Calculate mean acc and mean gyr over each window
magnitude_windows_acc AS (
    SELECT
        toStartOfSecond(ts) AS time_window,
        sqrt(avg(x*x + y*y + z*z)) AS acc_mean_magnitude
    FROM acc
    GROUP BY time_window
    ORDER BY time_window
),

magnitude_windows_gyr AS (
    SELECT
        toStartOfSecond(ts) AS time_window,
        sqrt(avg(x*x + y*y + z*z)) AS gyr_mean_magnitude
    FROM gyr
    GROUP BY time_window
    ORDER BY time_window
),

magnitude_per_window AS (
    SELECT
        a.time_window,
        a.acc_mean_magnitude,
        g.gyr_mean_magnitude
    FROM magnitude_windows_acc AS a
    INNER JOIN magnitude_windows_gyr AS g
        ON a.time_window = g.time_window
    ORDER BY a.time_window
),

-- Calculate if mean shows a change in direction
direction_change AS (
    SELECT
        time_window,
        acc_mean_magnitude,
        gyr_mean_magnitude,

        -- Difference between current and previous value (row)
        abs(acc_mean_magnitude - lagInFrame(acc_mean_magnitude) OVER (ORDER BY time_window)) AS acc_change,
        abs(gyr_mean_magnitude - lagInFrame(gyr_mean_magnitude) OVER (ORDER BY time_window)) AS gyr_change,

        -- check if there is a change in direction
        if(
            lagInFrame(acc_mean_magnitude) OVER (ORDER BY time_window) IS NULL,
            0,
            if(
                (acc_mean_magnitude - lagInFrame(acc_mean_magnitude) OVER (ORDER BY time_window))
                * (lagInFrame(acc_mean_magnitude) OVER (ORDER BY time_window)
                   - lagInFrame(acc_mean_magnitude, 2) OVER (ORDER BY time_window)) < 0,
                1,
                0
            )
        ) AS acc_direction_change,

        if(
            lagInFrame(gyr_mean_magnitude) OVER (ORDER BY time_window) IS NULL,
            0,
            if(
                (gyr_mean_magnitude - lagInFrame(gyr_mean_magnitude) OVER (ORDER BY time_window))
                * (lagInFrame(gyr_mean_magnitude) OVER (ORDER BY time_window)
                   - lagInFrame(gyr_mean_magnitude, 2) OVER (ORDER BY time_window)) < 0,
                1,
                0
            )
        ) AS gyr_direction_change
    FROM magnitude_per_window
),

-- get average acc and gyr change over neighboring rows
-- sum the amount of times a change in direction has occured
aggregate_data AS (
    SELECT
        time_window,
        acc_mean_magnitude,
        gyr_mean_magnitude,
        acc_change,
        gyr_change,
        acc_direction_change,
        gyr_direction_change,

        avg(acc_change) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_acc_change,
        avg(gyr_change) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_gyr_change,

        sum(acc_direction_change) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS acc_direction_change_rate,
        sum(gyr_direction_change) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS gyr_direction_change_rate
    FROM direction_change
),

-- check if the avverage acc change and average gyr change are over threshold
-- and average direction change within threshold
detect_tremor AS (
    SELECT
        time_window,
        acc_change,
        gyr_change,
        acc_direction_change,
        gyr_direction_change,
        avg_acc_change,
        avg_gyr_change,
        acc_direction_change_rate,
        gyr_direction_change_rate,

        if(
            (avg_acc_change > 3 OR avg_gyr_change > 15)
            AND (acc_direction_change_rate BETWEEN 2 AND 20
                 OR gyr_direction_change_rate BETWEEN 2 AND 20),
            1, 0
        ) AS tremor_detected
    FROM aggregate_data
)

--display number of tremors per day
SELECT
    toStartOfDay(time_window) AS date_time,
    -- round to closest 100-number to account for different
    -- handling of rounding numbers across engines
    floor(sum(tremor_detected) / 100) * 100 AS tremors_per_day
FROM detect_tremor
GROUP BY date_time
ORDER BY date_time;
