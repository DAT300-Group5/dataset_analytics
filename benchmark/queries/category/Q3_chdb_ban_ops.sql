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

WITH HR_intervals AS
(SELECT 
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
    AVG(HR) as interval_HR
FROM hrm
GROUP BY time_interval),

GYR_intervals AS
(SELECT 
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
    AVG(x*x + y*y + z*z) AS gyr_magnitude
FROM gyr
GROUP BY time_interval),

ACC_intervals AS
(SELECT 
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
    AVG(x*x + y*y + z*z) AS acc_magnitude
FROM acc
GROUP BY time_interval)

SELECT 
    h.time_interval AS time_interval, h.interval_HR, 
    a.acc_magnitude, g.gyr_magnitude,
    CASE
        WHEN h.interval_HR < 80 THEN
            CASE WHEN a.acc_magnitude < 90 AND g.gyr_magnitude < 5000 THEN 'sitting'
            ELSE 'light_activity' END

        WHEN h.interval_HR < 110 THEN
            CASE WHEN a.acc_magnitude < 90 AND g.gyr_magnitude < 5000 THEN 'sitting'
            ELSE 'light_activity' END

        WHEN h.interval_HR >= 110 THEN 
            CASE WHEN a.acc_magnitude > 110 AND g.gyr_magnitude > 8000 THEN 'heavy_activity'
            ELSE 'light_activity' END
        ELSE 'misc'  
    END AS type_of_activity 
FROM HR_intervals as h 
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
WHERE h.time_interval BETWEEN toDateTime('2021-03-14 00:00:00') AND toDateTime('2021-03-21 23:59:59')
ORDER BY h.time_interval;
