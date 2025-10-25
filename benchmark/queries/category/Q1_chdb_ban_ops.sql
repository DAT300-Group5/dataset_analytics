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
    AVG(HR) AS interval_HR
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
GROUP BY time_interval),

LIT_intervals AS
(SELECT 
    toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
    CASE WHEN AVG(ambient_light_intensity) > 100 THEN 1 ELSE 0 END as is_light
FROM lit
GROUP BY time_interval)

SELECT 
    h.time_interval AS time_interval,
    h.interval_HR, a.acc_magnitude, g.gyr_magnitude,
    CASE 
        WHEN h.interval_HR < 80 OR (a.acc_magnitude < 2 AND g.gyr_magnitude < 2) THEN 'sitting' 
        WHEN h.interval_HR < 110 OR (a.acc_magnitude < 10 AND g.gyr_magnitude < 10) THEN 'light_activity'
        WHEN h.interval_HR >= 110 OR (a.acc_magnitude < 100 AND g.gyr_magnitude < 100) THEN 'heavy_activity'
        ELSE 'misc' END AS type_of_activity
FROM HR_intervals as h 
JOIN LIT_intervals l ON h.time_interval = l.time_interval
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
WHERE l.is_light = 1
ORDER BY h.time_interval;