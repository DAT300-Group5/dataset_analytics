# Execution Plan and Optimizers

## SQLite

### Execution Plan

`EXPLAIN`, `EXPLAIN QUERY PLAN`.

```sql
EXPLAIN SELECT * FROM users WHERE age > 30;
```

```sql
EXPLAIN QUERY PLAN
SELECT * FROM users WHERE age > 30 AND city = 'Paris';
```

`EXPLAIN QUERY PLAN` is fast, as it doesn't run the SQL actually.

SQLite's CLI has dot command - `.eqp on` can also open `EXPLAIN QUERY PLAN`.

[EXPLAIN QUERY PLAN](https://sqlite.org/eqp.html)

[Explanations of the operators as seen in the SQLite query execution plan](https://use-the-index-luke.com/sql/explain-plan/sqlite/operations)

### Optimizers

Could be optimizers: [Pragma statements supported by SQLite](https://sqlite.org/pragma.html)

The `PRAGMA optimize`, does have some optimizers, but none of them is related to analytical SQL.

In other words, if your workload is mainly read-only analytical, `PRAGMA optimize;` will hardly bring you any performance improvement.

In addition, not only SQLite, but also various database engines' configurable items will include options for how many resources the database engine can use, such as setting `cache_size`, `mmap_size`, and so on. I don't think blindly increasing these settings in resource-constrained situations will be helpful - it may even be harmful, such as common jitter and paging problems in the operating system.

---

Regarding the Join algorithm:

The "three Join algorithms" - **Nested-Loop / Merge / Hash**

**SQLite actually only has Nested-Loop Join** (inner loop + index/table access); it **does not have** the traditional Merge Join or Hash Join operators that you can switch to.

## DuckDB

### Execution Plan

`EXPLAIN your_query;`, `EXPLAIN ANALYZE your_query;`.

- `EXPLAIN`: Only provides the logical/physical execution plan (tree structure).
- `EXPLAIN ANALYZE`: **Actually executes the query** and returns information such as the time, row count, estimated vs. actual for each operator.

Could use with `PRAGMA explain_output = 'optimized_only';`.

---

Or choose:

```sql
PRAGMA enable_profiling='json';
SET profiling_output='profiling_query_1.json';
```

Then run your query.

DuckDB will write the execution plan and detailed performance metrics of each operator (including time consumption, rows returned, rows scanned, bytes read, etc.) to the JSON file you specified.

This JSON is used for **automation analysis / visualization tools / post-processing**, such as: 

- Parse the results using Python;
- Compare the performance of multiple queries;
- Create dashboards, reports, etc.

### Optimizers

- [Optimizers: The Low-Key MVP – DuckDB](https://duckdb.org/2024/11/14/optimizers)
- [Tuning Workloads – DuckDB](https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads)

1. The optimizer in DuckDB can be easily disabled by using `PRAGMA disable_optimizer;`.
2. List the optimizer rules using the built-in table function: `SELECT name FROM duckdb_optimizers() ORDER BY 1;` `

```bash
name
build_side_probe_side
column_lifetime
common_aggregate
common_subexpressions
compressed_materialization
cte_filter_pusher
cte_inlining
deliminator
duplicate_groups
empty_result_pullup
expression_rewriter
extension
filter_pullup
filter_pushdown
in_clause
join_filter_pushdown
join_order
late_materialization
limit_pushdown
materialized_cte
regex_range
reorder_filter
sampling_pushdown
statistics_propagation
sum_rewriter
top_n
unnest_rewriter
unused_columns
```

- `filter_*`, `limit_pushdown`, `join_order` → Control the execution order and data flow.
- `*_rewriter`, `common_*` → Rewrite SQL logical expressions.
- `column_*`, `unused_columns`, `late_materialization` → Reduce memory overhead.
- `statistics_propagation` → Provide a basis for cost estimation.

You can turn off or enable any one of them separately, for example:

```sql
SET disabled_optimizers='filter_pushdown,join_order';
```

Then use:

```sql
EXPLAIN SELECT ...;
```

Observe the changes in the execution plan to see which step no longer occurs. 

The design of DuckDB is highly modular, and these 28 optimizers almost cover all its lightweight optimization stages from logical to physical.

#### Expression-level Optimizations

| Optimizer                 | Description                                                                                         |
| ------------------------- | --------------------------------------------------------------------------------------------------- |
| **expression_rewriter**   | Rewrites expressions (e.g., constant folding, arithmetic simplification).                           |
| **common_subexpressions** | Extracts common subexpressions to avoid redundant computation.                                      |
| **sum_rewriter**          | Rewrites aggregate functions like `SUM(x)` into more efficient forms (e.g., `SUM(1)` → `COUNT(*)`). |
| **regex_range**           | Optimizes range scans for `LIKE` / `REGEXP` expressions.                                            |
| **in_clause**             | Rewrites `IN (...)` clauses into hash lookups or semi-joins.                                        |
| **unnest_rewriter**       | Optimizes expression structures involving `UNNEST` or list expansion.                               |

#### Predicate and Filter Optimizations

| Optimizer                | Description                                                    |
| ------------------------ | -------------------------------------------------------------- |
| **filter_pushdown**      | Pushes predicates down to the scan stage (predicate pushdown). |
| **filter_pullup**        | Pulls filters upward to merge multiple filtering layers.       |
| **reorder_filter**       | Reorders multiple filter conditions to improve selectivity.    |
| **join_filter_pushdown** | Pushes filter conditions down to one side of a join.           |
| **cte_filter_pusher**    | Pushes filters into Common Table Expressions (CTEs).           |
| **limit_pushdown**       | Pushes `LIMIT` down into subqueries, CTEs, or scans.           |
| **sampling_pushdown**    | Pushes `SAMPLE` operations earlier to reduce data volume.      |

#### Join and Plan Reordering

| Optimizer                 | Description                                                 |
| ------------------------- | ----------------------------------------------------------- |
| **join_order**            | Join order optimizer — determines join order and algorithm. |
| **build_side_probe_side** | Decides the build/probe sides in hash joins.                |
| **duplicate_groups**      | Eliminates duplicate grouping clauses.                      |
| **common_aggregate**      | Pushes down or merges identical aggregate operations.       |

#### CTE and Subquery Optimizations

| Optimizer               | Description                                                         |
| ----------------------- | ------------------------------------------------------------------- |
| **cte_inlining**        | Inlines CTEs to eliminate materialization.                          |
| **materialized_cte**    | Controls which CTEs should be materialized (kept as temp results).  |
| **empty_result_pullup** | Detects subqueries that produce no results and returns empty early. |

#### Column and Storage Optimizations

| Optimizer                      | Description                                                                      |
| ------------------------------ | -------------------------------------------------------------------------------- |
| **column_lifetime**            | Analyzes column lifetimes to release unused data early.                          |
| **unused_columns**             | Removes unused columns from queries (column pruning).                            |
| **late_materialization**       | Delays column data loading until actually needed.                                |
| **compressed_materialization** | Uses compressed representations for intermediate results to reduce memory usage. |

#### Statistics and Inference

| Optimizer                  | Description                                                                 |
| -------------------------- | --------------------------------------------------------------------------- |
| **statistics_propagation** | Propagates column statistics (e.g., min/max, cardinality) up the plan tree. |
| **deliminator**            | Internal stage separator used for optimization phase splitting.             |
| **extension**              | Plugin entry point for custom optimizer extensions.                         |
| **top_n**                  | Specialized “Top-N sorting” optimization (uses heap instead of full sort).  |

## chDB

### Execution Plan

[Understanding Query Execution with the Analyzer | ClickHouse Docs](https://clickhouse.com/docs/guides/developer/understanding-query-execution-with-the-analyzer#planner)

Just like DuckDB, it could use `EXPLAIN your_query;`, `EXPLAIN PLAN ANALYZE your_query;`.

### Optimizers

ClickHouse has an optimizer mechanism: it supports predicate pushdown, join algorithm selection, connection order optimization, etc., all of which are part of the optimizer.

However, chDB does not have such detailed documentation, so we hope to check the ClickHouse documentation to confirm if there are similar configurations, and then directly test them on chDB.

Ref：

- [Session Settings | ClickHouse Docs](https://clickhouse.com/docs/operations/settings/settings)
- [system.settings | ClickHouse Docs](https://clickhouse.com/docs/operations/system-tables/settings)
- [holistic-performance-optimization | ClickHouse Docs](https://clickhouse.com/docs/academic_overview#4-4-holistic-performance-optimization)
- [Guide for Query optimization | ClickHouse Docs](https://clickhouse.com/docs/optimize/query-optimization#basic-optimization)

Check how many ClickHouse mechanisms chDB actually supports:

```sql
SELECT * FROM system.settings;
```

Check how many optimization items there are:

```sql
SELECT name, value, description
FROM system.settings
WHERE name LIKE '%optimize%' OR name LIKE '%optimizer%';
```

Check currently open:

```sql
SELECT name, value, description
FROM system.settings
WHERE (name LIKE '%optimize%' OR name LIKE '%optimizer%')
  AND value IN ('1', 'true');
```

Only look at the items that have been modified:

```sql
SELECT name, value, changed, default_value
FROM system.settings
WHERE changed AND (name LIKE '%optimize%' OR name LIKE '%optimizer%');
```

Select some of the contents and complete a setting that disables most optimizations:

```sql
SET enable_optimize_predicate_expression = 0;
SET query_plan_optimize_prewhere         = 0;
SET optimize_move_to_prewhere            = 0;

SET optimize_read_in_order               = 0;
SET optimize_read_in_window_order        = 0;
SET optimize_aggregation_in_order        = 0;

SET optimize_functions_to_subcolumns     = 0;
SET optimize_time_filter_with_preimage   = 0;
SET optimize_extract_common_expressions  = 0;
SET optimize_uniq_to_count               = 0;
SET optimize_rewrite_sum_if_to_count_if  = 0;
SET optimize_rewrite_aggregate_function_with_if = 0;
SET optimize_injective_functions_in_group_by     = 0;
SET optimize_group_by_function_keys      = 0;
SET optimize_group_by_constant_keys      = 0;
SET optimize_normalize_count_variants    = 0;

SET optimize_trivial_count_query         = 0;
SET optimize_count_from_files            = 0;

SET optimize_use_projections             = 0;
SET optimize_use_implicit_projections    = 0;
SET force_optimize_projection            = 0;
```

