
--display number of tremors per day
SELECT
  (CAST(time_window AS INTEGER)/86400000)*86400000 AS time_window_day,
  sum(tremor_detected) as tremors_per_day
FROM (
    SELECT
        time_window,
        avg_acc_change,
        avg_gyr_change,
        acc_direction_change_rate,
        gyr_direction_change_rate,
        CASE
            WHEN (avg_acc_change > 3 OR avg_gyr_change > 15)
             AND (acc_direction_change_rate BETWEEN 2 AND 20 OR gyr_direction_change_rate BETWEEN 2 AND 20)
            THEN 1 ELSE 0
        END AS tremor_detected
    FROM (
        -- Compute rolling averages and sums (1 row preceding)
        SELECT
        time_window,
        acc_mean_magnitude,
        gyr_mean_magnitude,
        avg(acc_change) OVER w2 AS avg_acc_change,
        avg(gyr_change) OVER w2 AS avg_gyr_change,
        sum(acc_direction_change) OVER w2 AS acc_direction_change_rate,
        sum(gyr_direction_change) OVER w2 AS gyr_direction_change_rate
        FROM 
        -- Calculate if mean shows a change in direction
        (
            SELECT
                time_window,
                acc_mean_magnitude,
                gyr_mean_magnitude,

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

            -- get time windows with magnitude for both acc and gyr
            FROM (
                SELECT
                    a.time_window,
                    a.acc_mean_magnitude,
                    g.gyr_mean_magnitude
                FROM 
                (
                    -- Divide dataset acc into 1 second windows for analysis
                    -- Calculate mean acc over each window 
                    SELECT 
                    (CAST(ts AS INTEGER)/1000)*1000 AS time_window,
                    sqrt(avg(x*x + y*y + z*z)) AS acc_mean_magnitude
                FROM acc
                GROUP BY 1) a
                INNER JOIN 
                (
                    -- Divide dataset gyr into 1 second windows for analysis
                    -- Calculate mean gyr over each window 
                    SELECT 
                    (CAST(ts AS INTEGER)/1000)*1000 AS time_window,
                    sqrt(avg(x*x + y*y + z*z)) AS gyr_mean_magnitude
                FROM gyr
                GROUP BY 1) g
                    USING (time_window)
            )
            WINDOW w AS (ORDER BY time_window)
        )
    WINDOW w2 AS (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
    )
)
GROUP BY time_window_day
ORDER BY time_window_day;
