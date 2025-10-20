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

[The SQLite Query Optimizer Overview](https://www.sqlite.org/optoverview.html)

SQLite’s optimizer does include several mechanisms that can be helpful for analytical workloads, particularly:

- Subquery flattening
- GROUP BY / ORDER BY optimizations
- Covering index and range scan optimizations
- Expression simplification and constant folding

However, its overall architecture makes it less suitable for handling large-scale aggregations, complex joins, window functions, or highly concurrent analytical queries.

The documentation explains many aspects of what the optimizer does, but it does **not** provide fine-grained switches like those available in ClickHouse or DuckDB.

Looking at the [Pragma statements supported by SQLite](https://sqlite.org/pragma.html), the available controls are quite limited. For example,
`PRAGMA automatic_index = ON|OFF` determines whether SQLite automatically creates transient indexes for equality joins and similar cases.

---

In addition, not only SQLite, but also various database engines' configurable items will include options for how many resources the database engine can use, such as setting `cache_size`, `mmap_size`, and so on. I don't think blindly increasing these settings in resource-constrained situations will be helpful - it may even be harmful, such as common jitter and paging problems in the operating system.

---

Regarding the Join algorithm:

The "three Join algorithms" - **Nested-Loop / Merge / Hash**

**SQLite actually only has Nested-Loop Join** (inner loop + index/table access); it **does not have** the traditional Merge Join or Hash Join operators that you can switch to.

> <https://www.sqlite.org/optoverview.html>
>
> 14.1. Hash Joins
>
> An automatic index is almost the same thing as a hash join. The only difference is that a B-Tree is used instead of a hash table. If you are willing to say that the transient B-Tree constructed for an automatic index is really just a fancy hash table, then a query that uses an automatic index is just a hash join.
>
> SQLite constructs a transient index instead of a hash table in this instance because it already has a robust and high performance B-Tree implementation at hand, whereas a hash-table would need to be added. Adding a separate hash table implementation to handle this one case would increase the size of the library (which is designed for use on low-memory embedded devices) for minimal performance gain. SQLite might be enhanced with a hash-table implementation someday, but for now it seems better to continue using automatic indexes in cases where client/server database engines might use a hash join.

## DuckDB

### Execution Plan

Recommend!: <https://db.cs.uni-tuebingen.de/explain/>

---

`EXPLAIN your_query;`, `EXPLAIN ANALYZE your_query;`.

- `EXPLAIN`: Only provides the logical/physical execution plan (tree structure).
- `EXPLAIN ANALYZE`: **Actually executes the query** and returns information such as the time, row count, estimated vs. actual for each operator.

You can limit the output to the optimized logical plan with `PRAGMA explain_output='optimized_only';`.

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
2. List the optimizer rules using the built-in table function: `SELECT name FROM duckdb_optimizers() ORDER BY 1;`

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

It could use `EXPLAIN your_query;`, `EXPLAIN PLAN ANALYZE your_query;`.

### Optimizers

ClickHouse has an optimizer mechanism: it supports predicate pushdown, join algorithm selection, connection order optimization, etc., all of which are part of the optimizer.

However, chDB does not have such detailed documentation, so we hope to check the ClickHouse documentation to confirm if there are similar configurations, and then directly test them on chDB.

Ref：

- [Session Settings | ClickHouse Docs](https://clickhouse.com/docs/operations/settings/settings)
- [system.settings | ClickHouse Docs](https://clickhouse.com/docs/operations/system-tables/settings)
- [holistic-performance-optimization | ClickHouse Docs](https://clickhouse.com/docs/academic_overview#4-4-holistic-performance-optimization)
- [Guide for Query optimization | ClickHouse Docs](https://clickhouse.com/docs/optimize/query-optimization#basic-optimization)

#### How to check

Check how many ClickHouse mechanisms chDB actually supports:

```sql
SELECT * FROM system.settings;
```

Check currently open:

```sql
SELECT name FROM system.settings
WHERE value IN ('1', 'true');
```

Only look at the items that have been modified:

```sql
SELECT name, value, changed, default_value
FROM system.settings
WHERE changed;
```

#### Description (Relevant)

| Setting                                       | Description                                                                                |
| --------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `enable_optimize_predicate_expression`        | Enables optimization of predicate expressions such as constant folding and simplification. |
| `query_plan_optimize_prewhere`                | Enables the PREWHERE optimization at the query plan stage.                                 |
| `optimize_move_to_prewhere`                   | Automatically moves eligible conditions from WHERE to PREWHERE.                            |
| `optimize_move_to_prewhere_if_final`          | Enables PREWHERE optimization for queries with the FINAL modifier.                         |
| `optimize_read_in_order`                      | Enables ordered read optimization when data is sorted by primary or sorting key.           |
| `optimize_read_in_window_order`               | Optimizes reading for window functions by using sorted input order.                        |
| `optimize_aggregation_in_order`               | Enables ordered aggregation optimization to reduce memory usage.                           |
| `optimize_functions_to_subcolumns`            | Allows function calls to be optimized to read subcolumns directly (e.g., JSON fields).     |
| `optimize_time_filter_with_preimage`          | Enables optimized time filtering with preimage transformation.                             |
| `optimize_extract_common_expressions`         | Extracts and reuses common subexpressions to avoid redundant computation.                  |
| `optimize_uniq_to_count`                      | Rewrites `uniq()` functions to `count()` when possible.                                    |
| `optimize_rewrite_sum_if_to_count_if`         | Rewrites `sumIf()` to `countIf()` when appropriate.                                        |
| `optimize_rewrite_aggregate_function_with_if` | Rewrites aggregate functions containing IF conditions into equivalent simpler forms.       |
| `optimize_injective_functions_in_group_by`    | Allows injective functions (like `toInt32()`) to be optimized in GROUP BY clauses.         |
| `optimize_group_by_function_keys`             | Optimizes GROUP BY expressions containing functions.                                       |
| `optimize_group_by_constant_keys`             | Optimizes GROUP BY with constant expressions by simplifying aggregation.                   |
| `optimize_normalize_count_variants`           | Normalizes different COUNT function variants to a common form.                             |
| `optimize_trivial_count_query`                | Enables fast-path optimization for simple `SELECT count()` queries.                        |
| `optimize_count_from_files`                   | Allows retrieving row counts directly from file metadata instead of scanning.              |
| `optimize_use_projections`                    | Enables the use of materialized projections for query optimization.                        |
| `optimize_use_implicit_projections`           | Enables implicit projections automatically inferred by the optimizer.                      |
| `force_optimize_projection`                   | Forces the optimizer to use available projections when applicable.                         |
| `allow_general_join_planning`                 | Enables the general join planning algorithm for complex JOINs.                             |
| `cross_to_inner_join_rewrite`                 | Rewrites CROSS JOINs to INNER JOINs when join conditions exist.                            |

#### Select optimizations to disable

Based on these configurable optimizer settings, we can selectively disable most of them to observe the baseline performance.

```sql
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
```
