-- Disable All Major Optimizations (Safe for Analytical Load Experiments)

-- Expression / Predicate Optimizations
SET enable_optimize_predicate_expression = 0;
SET query_plan_optimize_prewhere         = 0;
SET optimize_move_to_prewhere            = 0;
SET optimize_move_to_prewhere_if_final   = 0;

-- Scan / Read Order Optimizations
SET optimize_read_in_order               = 0;
SET optimize_read_in_window_order        = 0;

-- Aggregation / Group By Optimizations
SET optimize_aggregation_in_order        = 0;
SET optimize_injective_functions_in_group_by     = 0;
SET optimize_group_by_function_keys      = 0;
SET optimize_group_by_constant_keys      = 0;
SET optimize_normalize_count_variants    = 0;
SET optimize_trivial_count_query         = 0;
SET optimize_count_from_files            = 0;
SET optimize_uniq_to_count               = 0;
SET optimize_rewrite_sum_if_to_count_if  = 0;
SET optimize_rewrite_aggregate_function_with_if = 0;

-- Expression / Subcolumn / Common Expression Optimizations
SET optimize_functions_to_subcolumns     = 0;
SET optimize_time_filter_with_preimage   = 0;
SET optimize_extract_common_expressions  = 0;

-- Projection / Storage-level Optimizations
SET optimize_use_projections             = 0;
SET optimize_use_implicit_projections    = 0;
SET force_optimize_projection            = 0;

-- Join / Rewrite Optimizations
SET allow_general_join_planning          = 0;
SET cross_to_inner_join_rewrite          = 0;

-- Note: These settings can be safely applied in a session context.
-- They affect only query optimization, not query correctness or stability.


USE sensor;

--display number of tremors per day
SELECT
    toStartOfDay(time_window) AS date_time,
    -- round to closest 100-number to account for different
    -- handling of rounding numbers across engines
    floor(sum(tremor_detected) / 100) * 100 AS tremors_per_day
FROM (
    -- check if the avverage acc change and average gyr change are over threshold
    -- and average direction change within threshold
    SELECT
        time_window,
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
    FROM (
        -- get average acc and gyr change over neighboring rows
        -- sum the amount of times a change in direction has occured
        SELECT
            time_window,
            acc_mean_magnitude,
            gyr_mean_magnitude,

            avg(acc_change) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_acc_change,
            avg(gyr_change) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_gyr_change,

            sum(acc_direction_change) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS acc_direction_change_rate,
            sum(gyr_direction_change) OVER (ORDER BY time_window ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS gyr_direction_change_rate
        FROM (
            -- Calculate if mean shows a change in direction
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
            FROM (
                SELECT
                    a.time_window,
                    a.acc_mean_magnitude,
                    g.gyr_mean_magnitude
                FROM (
                    -- Divide dataset into 1 second windows for analysis
                    -- Calculate mean acc and mean gyr over each window
                    SELECT
                        toStartOfSecond(ts) AS time_window,
                        sqrt(avg(x*x + y*y + z*z)) AS acc_mean_magnitude
                    FROM acc
                    GROUP BY time_window
                    ORDER BY time_window
                ) AS a
                INNER JOIN (
                    SELECT
                        toStartOfSecond(ts) AS time_window,
                        sqrt(avg(x*x + y*y + z*z)) AS gyr_mean_magnitude
                    FROM gyr
                    GROUP BY time_window
                    ORDER BY time_window
                ) AS g
                    ON a.time_window = g.time_window
                ORDER BY a.time_window
            )
        )
    )
)
GROUP BY date_time
ORDER BY date_time;
