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

WITH
hrm_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(ts) AS minute_dt,
    avg(HR) AS avg_hr
  FROM hrm
  GROUP BY deviceId, minute_dt
),
ppg_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(ts) AS minute_dt,
    avg(ppg) AS avg_ppg
  FROM ppg
  GROUP BY deviceId, minute_dt
),
acc_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(ts) AS minute_dt,
    sqrt(avg(x*x + y*y + z*z)) AS rms_acc
  FROM acc
  GROUP BY deviceId, minute_dt
),
ped_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(ts) AS minute_dt,
    sum(steps) AS total_steps
  FROM ped
  GROUP BY deviceId, minute_dt
),
lit_minute AS (
  SELECT
    deviceId,
    toStartOfMinute(ts) AS minute_dt,
    quantileExactInclusive(0.5)(ambient_light_intensity) AS median_light
  FROM lit
  GROUP BY deviceId, minute_dt
),
minutes AS (
  SELECT deviceId, minute_dt FROM hrm_minute
  UNION DISTINCT
  SELECT deviceId, minute_dt FROM ppg_minute
  UNION DISTINCT
  SELECT deviceId, minute_dt FROM acc_minute
  UNION DISTINCT
  SELECT deviceId, minute_dt FROM ped_minute
  UNION DISTINCT
  SELECT deviceId, minute_dt FROM lit_minute
)
SELECT
  m.deviceId AS deviceId,
  formatDateTime(m.minute_dt, '%Y-%m-%dT%H:%i:%S+00:00') AS minute_ts,
  COALESCE(h.avg_hr,       0.0) AS avg_hr,
  COALESCE(p.avg_ppg,      0.0) AS avg_ppg,
  COALESCE(a.rms_acc,      0.0) AS rms_acc,
  COALESCE(d.total_steps,  0  ) AS total_steps,
  COALESCE(l.median_light, 0.0) AS median_light
FROM minutes AS m
LEFT JOIN hrm_minute AS h USING (deviceId, minute_dt)
LEFT JOIN ppg_minute AS p USING (deviceId, minute_dt)
LEFT JOIN acc_minute AS a USING (deviceId, minute_dt)
LEFT JOIN ped_minute AS d USING (deviceId, minute_dt)
LEFT JOIN lit_minute AS l USING (deviceId, minute_dt)
ORDER BY m.deviceId, m.minute_dt;
