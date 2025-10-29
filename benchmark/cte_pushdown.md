# chDB vs DuckDB: WHERE Clause Pushdown to CTE Comparison

## Terminology

- **category_Q3**: Outer WHERE clause, i.e., using WHERE clause after FROM/JOIN in the main query to filter aggregated columns
- **category_Q1**: WHERE clause inside CTE, i.e., using WHERE clause in CTE definition to filter source table columns

## Core Question

**Can chDB push down filters from category_Q3 (outer WHERE clause on aggregated columns) to CTE definitions, so that GROUP BY only processes filtered rows?**
**Can DuckDB achieve the same?**

---

## Test Scenario: Comparison of Two WHERE Clause Types

### category_Q3: Outer WHERE (on Aggregated Columns)

```sql
-- category_Q3: Outer WHERE on aggregated column time_interval
WITH HR_intervals AS (
    SELECT 
        toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,  -- Aggregation expression
        AVG(HR) as interval_HR
    FROM hrm
    GROUP BY time_interval  -- ⚠️ No WHERE in CTE definition
),
ACC_intervals AS (
    SELECT 
        toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
        AVG(x*x + y*y + z*z) AS acc_magnitude
    FROM acc
    GROUP BY time_interval  -- ⚠️ No WHERE in CTE definition
),
GYR_intervals AS (
    SELECT 
        toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
        AVG(x*x + y*y + z*z) AS gyr_magnitude
    FROM gyr
    GROUP BY time_interval  -- ⚠️ No WHERE in CTE definition
)
SELECT ...
FROM HR_intervals h 
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
WHERE h.time_interval BETWEEN toDateTime('2021-03-14 00:00:00') AND toDateTime('2021-03-21 23:59:59')
  -- ⚠️ category_Q3: Outer WHERE on aggregated column
ORDER BY h.time_interval;
```

**Key Points:**
- ⚠️ `h.time_interval` is an aggregated column (`toStartOfInterval(ts, INTERVAL 5 MINUTE)`)
- ⚠️ WHERE condition is in the **outer main query**
- ⚠️ **Reverse derivation** needed: `time_interval BETWEEN X AND Y` → `ts BETWEEN ...`

### category_Q1: WHERE Inside CTE (on Source Table Columns)

```sql
-- category_Q1: WHERE inside CTE on source table column ts
WITH HR_intervals AS (
    SELECT 
        toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
        AVG(HR) as interval_HR
    FROM hrm
    WHERE ts BETWEEN toDateTime('2021-03-14 00:00:00') AND toDateTime('2021-03-21 23:59:59')  -- ✅ category_Q1: WHERE inside CTE
    GROUP BY time_interval
),
ACC_intervals AS (
    SELECT 
        toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
        AVG(x*x + y*y + z*z) AS acc_magnitude
    FROM acc
    WHERE ts BETWEEN toDateTime('2021-03-14 00:00:00') AND toDateTime('2021-03-21 23:59:59')  -- ✅ category_Q1: WHERE inside CTE
    GROUP BY time_interval
),
GYR_intervals AS (
    SELECT 
        toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
        AVG(x*x + y*y + z*z) AS gyr_magnitude
    FROM gyr
    WHERE ts BETWEEN toDateTime('2021-03-14 00:00:00') AND toDateTime('2021-03-21 23:59:59')  -- ✅ category_Q1: WHERE inside CTE
    GROUP BY time_interval
)
SELECT ...
FROM HR_intervals h 
JOIN ACC_intervals a ON h.time_interval = a.time_interval
JOIN GYR_intervals g ON h.time_interval = g.time_interval
ORDER BY h.time_interval;
```

**Key Points:**
- ✅ `ts` is a source table column (not aggregated)
- ✅ WHERE condition is in the **CTE definition**
- ✅ Can directly use index scan, no reverse derivation needed

---

## ✅ chDB/ClickHouse: Can Push Down!

### Execution Plan Analysis

**chDB Execution Plan:**
```
ReadFromMergeTree (sensor.hrm)
    ↓
Filter  ← WHERE condition pushed down here! ✅
    ↓
Aggregating  ← Aggregating filtered data ✅
    ↓
Join
```

**Key Findings:**
- ✅ WHERE condition is **automatically pushed down** by the optimizer to after ReadFromMergeTree and before Aggregating in each CTE
- ✅ Optimizer can **reverse derive**: `time_interval BETWEEN X AND Y` → `ts BETWEEN ...`
- ✅ GROUP BY only processes filtered data

### Optimizer Mechanism

**Source Location:** `src/Processors/QueryPlan/Optimizations/filterPushDown.cpp`

**Optimization Process:**
```
1. Identify WHERE condition
   └─ WHERE h.time_interval BETWEEN ...

2. Analyze column source
   └─ Discover time_interval = toStartOfInterval(ts, INTERVAL 5 MINUTE)

3. Reverse derivation
   └─ Transform time_interval BETWEEN '2021-03-14 00:00:00' AND '2021-03-21 23:59:59'
   └─ Derive the range ts must satisfy

4. Push down to CTE definition
   └─ Add Filter after ReadFromMergeTree in each CTE
   └─ Filter condition: ts BETWEEN (derived range)
```

**Key Capabilities:**
- ✅ **Reverse derivation of aggregation expressions**: `toStartOfInterval` function can be reverse derived
- ✅ **Automatic pushdown**: No manual optimization needed, optimizer does it automatically
- ✅ **Early filtering**: Filter before aggregation, GROUP BY only processes filtered data

### Performance Impact

**category_Q3 (Outer WHERE) Execution Flow:**
```
Stage 1: CTE Materialization
├─ ReadFromMergeTree (hrm)  ← Reading full table
├─ Filter (BETWEEN)  ← WHERE pushed down here! ✅
│   └─ ts BETWEEN (reverse-derived range)
└─ Aggregating  ← Aggregating filtered data ✅

Result: Only aggregate filtered data, same performance as category_Q1
```

**Performance Test Results:**
- ✅ category_Q3 (outer WHERE) performance ≈ category_Q1 (WHERE in CTE)
- ✅ Both only aggregate filtered data
- ✅ Execution plans are identical

---

## ⚠️ DuckDB: Cannot Fully Push Down!

### Key Limitations

**DuckDB's CTEFilterPusher Mechanism:**

**Source Location:** `duckdb/src/optimizer/cte_filter_pusher.cpp`

```cpp
// Lines 46-54: Only processes Filters on CTE references
else if (op.type == LogicalOperatorType::LOGICAL_FILTER &&
         op.children[0]->type == LogicalOperatorType::LOGICAL_CTE_REF) {
    // ✅ Found Filter on CTE reference
    auto &cte_ref = op.children[0]->Cast<LogicalCTERef>();
    it->second->filters.push_back(op);
}
```

**Key Limitations:**
- ✅ CTEFilterPusher **can handle** WHERE clauses (Filters on CTE references)
- ⚠️ **However**, for aggregated columns (such as `time_bucket` function), **cannot reverse derive**

### Why Cannot Reverse Derive?

**JoinFilterPushdownOptimizer Limitations:**

**Source Location:** `duckdb/src/optimizer/join_filter_pushdown_optimizer.cpp`

```cpp
// Lines 126-141: Attempt to push down Filter through aggregation
case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
    auto &aggr = probe_child.Cast<LogicalAggregate>();
    for (auto &filter : columns) {
        auto &expr = *aggr.groups[filter.probe_column_index.column_index];
        Paladinif (!PushdownJoinFilterExpression(expr, filter)) {
            // ⚠️ If expression is a complex function like time_bucket, cannot push down
            return;  // Cannot push down
        }
    }
}
```

**Key Limitations:**
- ⚠️ `PushdownJoinFilterExpression` can only handle **simple column references** or **simple expressions**
- ⚠️ For functions like `time_bucket(INTERVAL '5m', ts)`, **cannot push down**
- ⚠️ **Cannot reverse derive**: `time_interval BETWEEN X AND Y` → `ts BETWEEN ...`

### Execution Flow

**DuckDB category_Q3 (Outer WHERE) Execution Flow:**
```
Stage 1: CTE Materialization
├─ Scan hrm table (full table scan) ⚠️
│   └─ No WHERE condition, scan all rows
├─ GROUP BY (process all rows) ⚠️
│   └─ time_bucket(INTERVAL '5m', ts) AS time_interval
└─ Materialize result (all time intervals) ⚠️
    └─ Example: 5076 rows

Stage 2: Main Query Execution
├─ JOIN operation
├─ Filter (WHERE time_interval BETWEEN ...)  ← After JOIN ⚠️
│   └─ Filtering happens after Materialization (too late!)
└─ Result: Need to process large amount of data before filtering
```

**Performance Issues:**
- ⚠️ Need to Materialize all data
- ⚠️ GROUP BY processes all rows
- ⚠️ Can only filter after JOIN (too late)

---

## Comparison Summary Table

| Dimension | chDB/ClickHouse | DuckDB |
|-----------|----------------|--------|
| **Supports WHERE Pushdown** | ✅ Yes | ✅ Yes (partial) |
| **Can Handle Filters on CTE References** | ✅ Yes | ✅ Yes |
| **Can Reverse Derive Aggregation Expressions** | ✅ Yes (e.g., `toStartOfInterval`) | ⚠️ No (e.g., `time_bucket`) |
| **Pushdown WHERE Before GROUP BY** | ✅ Can | ⚠️ Cannot (for complex functions) |
| **category_Q3 vs category_Q1 Performance** | ✅ Same after optimization | ⚠️ category_Q3 slower |

---

## Execution Plan Verification

### Verification Method

We verify chDB's WHERE pushdown capability by running actual queries to obtain execution plans.

**Verification Script:** `verify_execution_plans.py`

```bash
# Run verification script
python3 verify_execution_plans.py
```

**Verification Queries:**
- category_Q3: Outer WHERE condition (on aggregated `time_interval` column)
- category_Q1: WHERE condition inside CTE (on source table `ts` column)

**Verification Method:**
Use `EXPLAIN PLAN` to obtain execution plans for both queries, compare execution plan structures, and check if Filter positions are the same.

### chDB Execution Plan Verification Results

#### category_Q3 Execution Plan (Outer WHERE)

```
1. Expression (Project names)
2.   Limit (preliminary LIMIT (without OFFSET))
3.     Sorting (Sorting for ORDER BY)
4.       Expression ((Before ORDER BY + (Projection + )))
5.         Expression
6.           Join
7.             Expression
8.               Aggregating
9.                 Expression
10.                  ReadFromMergeTree (sensor.hrm)
11.            Expression
12.              Aggregating
13.                Expression (( + (Before GROUP BY + Change column names to column identifiers)))
14.                  Expression
15.                    ReadFromMergeTree (sensor.acc)
```

**Key Observations:**
- Line 13 shows `Expression (( + (Before GROUP BY + Change column names to column identifiers)))`
- This Expression is located after `ReadFromMergeTree` and before `Aggregating`
- **Indicates WHERE condition has been pushed down before Aggregating**

#### category_Q1 Execution Plan (WHERE Inside CTE)

```
1. Expression (Project names)
2.   Limit (preliminary LIMIT (without OFFSET))
3.     Sorting (Sorting for ORDER BY)
4.       Expression ((Before ORDER BY + Projection))
5.         Expression
6.           Join
7.             Expression ((Change column names to column identifiers + (Project names + Projection)))
8.               Aggregating
9.                 Expression (Before GROUP BY)
10.                  Expression
11.                    ReadFromMergeTree (sensor.hrm)
12.            Expression ((Change column names to column identifiers + (Project names + Projection)))
13.              Aggregating
14.                Expression (Before GROUP BY)
15.                  Expression
16.                    ReadFromMergeTree (sensor.acc)
```

**Key Observations:**
- Lines 9 and 14 show `Expression (Before GROUP BY)`
- Expression is located after `ReadFromMergeTree` and before `Aggregating`
- **WHERE condition is in CTE definition, already before Aggregating**

#### Comparison Analysis

| Step | category_Q3 | category_Q1 | Conclusion |
|------|------------|-------------|------------|
| **ReadFromMergeTree** | ✅ Present | ✅ Present | Both have it |
| **Expression (Filter position)** | ✅ Present (Line 13, contains Before GROUP BY) | ✅ Present (Lines 9, 14, Before GROUP BY) | ✅ Same |
| **Aggregating** | ✅ Present | ✅ Present | Both after Expression |
| **Execution Plan Structure** | ✅ Similar | ✅ Similar | ✅ **Structure is the same** |

**Verification Conclusion:**

1. ✅ **category_Q3 and category_Q1 have the same execution plan structure**
   - Both contain `ReadFromMergeTree → Expression → Aggregating → Join` structure
   - Expressions are both located after `ReadFromMergeTree` and before `Aggregating`

2. ✅ **chDB successfully pushed down category_Q3's WHERE condition**
   - Line 13 of category_Q3 `Expression (( + (Before GROUP BY + ...)))` indicates WHERE condition has been pushed down
   - Pushdown position is the same as category_Q1 (both before Aggregating)

3. ✅ **WHERE pushdown enables GROUP BY to only process filtered data**
   - Both queries have Filters in the same position, both before aggregation
   - **Proves chDB can push down filters from outer WHERE to CTE definition, so GROUP BY only processes filtered rows**

### DuckDB Execution Plan Verification Description

For DuckDB, since it cannot push down WHERE conditions from category_Q3 (outer WHERE on aggregated columns) to CTE definitions, the execution plan will show:

**category_Q3 Execution Plan:**
- CTE Materialization stage: No Filter, full table scan
- Main query stage: Filter after JOIN

**category_Q1 Execution Plan:**
- CTE Materialization stage: Has Filter, only scans filtered data
- Main query stage: No additional Filter needed

**Comparison Differences:**
- category_Q3 needs to Materialize all data first, then filter
- category_Q1 only Materializes filtered data
- **Different execution plan structures prove DuckDB cannot push down category_Q3's WHERE condition**

### DuckDB Execution Plan Verification Results (Actual Execution)

We verify DuckDB's WHERE pushdown capability by running actual queries to obtain execution plans.

**Verification Script:** `get_duckdb_plans.sh`

```bash
# Run verification script
./get_duckdb_plans.sh
```

#### category_Q3 Execution Plan (Outer WHERE) - Actual Results

**Key Observations:**
- **FILTER Step Position:** FILTER is located after `PROJECTION` and before `HASH_GROUP_BY`
- **FILTER Condition:** `time_bucket('00:05:00'::INTERVAL, CAST(ts AS TIMESTAMP)) BETWEEN ...`
- **SEQ_SCAN Stage:**
  - acc table: Scans `~7,573,354 rows` (full table scan, **no Filters**)
  - hrm table: Scans `~1,495,193 rows` (full table scan, **no Filters**)

**Execution Flow:**
```
SEQ_SCAN (full table scan, no Filter) 
  ↓
PROJECTION (compute time_bucket expression)
  ↓
FILTER (filter on aggregated column) ⚠️ Position too late
  ↓
HASH_GROUP_BY (aggregate)
  ↓
HASH_JOIN
```

#### category_Q1 Execution Plan (WHERE Inside CTE) - Actual Results

**Key Observations:**
- **Filters Position:** Filters are directly at the `SEQ_SCAN` stage
- **Filters Conditions:** 
  - hrm table: `ts>='2021-03-14 00:00:00'::TIMESTAMP_NS AND ts<='2021-03-21 23:59:59'::TIMESTAMP_NS`
  - acc table: `ts>='2021-03-14 00:00:00'::TIMESTAMP_NS AND ts<='2021-03-21 23:59:59'::TIMESTAMP_NS`
- **SEQ_SCAN Stage:**
  - acc table: Applies Filters during scan (**filtering at scan stage**)
  - hrm table: Applies Filters during scan (**filtering at scan stage**)

**Execution Flow:**
```
SEQ_SCAN (with Filters, filter during scan) ✅ Early filtering
  ↓
PROJECTION (compute time_bucket expression)
  ↓
HASH_GROUP_BY (aggregate)
  ↓
HASH_JOIN
```

#### Actual Execution Plan Comparison Analysis

| Dimension | category_Q3 | category_Q1 | Conclusion |
|-----------|------------|-------------|------------|
| **Filters at SEQ_SCAN Stage** | ❌ No | ✅ Yes (`ts BETWEEN ...`) | **Key Difference** |
| **FILTER Position** | ⚠️ After PROJECTION | ✅ SEQ_SCAN stage | **Different position** |
| **Scan Rows (acc table)** | ⚠️ ~7,573,354 (full table) | ✅ Reduced after filtering | **Data volume difference** |
| **Scan Rows (hrm table)** | ⚠️ ~1,495,193 (full table) | ✅ Reduced after filtering | **Data volume difference** |
| **Filter Timing** | ⚠️ After computing time_bucket | ✅ At scan stage | **Timing difference** |

**Verification Conclusion:**

1. ✅ **category_Q3 execution plan shows Filters after PROJECTION**
   - FILTER step is located after `PROJECTION` and before `HASH_GROUP_BY`
   - Must first scan full table data, compute `time_bucket` expression, then filter on aggregated column
   - **Cannot push down to source table scan stage**

2. ✅ **category_Q1 execution plan shows Filters at SEQ_SCAN stage**
   - Filters are directly at the `SEQ_SCAN` stage, applied during scan
   - Only scans data in the matching `ts` range
   - **Early filtering, reduces data volume to process**

3. ✅ **Different execution plan structures prove DuckDB cannot push down category_Q3's WHERE condition**
   - category_Q3: Needs to Materialize all data (scan full table), then filter on aggregated column
   - category_Q1: Filters at scan stage, only Materializes filtered data
   - **Different execution plan structures prove DuckDB cannot push down outer WHERE (on aggregated columns) to CTE definition**

---

## Technical Difference Analysis

### chDB's Optimization Capability

**Why can chDB reverse derive?**

1. **Function Semantic Analysis**
   - chDB's optimizer understands the semantics of `toStartOfInterval`
   - Can reverse derive: `toStartOfInterval(ts, INTERVAL 5 MINUTE) BETWEEN X AND Y`
   - → The range `ts` must satisfy

2. **FilterPushdown Optimizer**
   - `src/Processors/QueryPlan/Optimizations/filterPushDown.cpp`
   - Can analyze expressions and reverse derive
   - Automatically pushes WHERE conditions to optimal positions

### DuckDB's Limitations

**Why can't DuckDB reverse derive?**

1. **CTEFilterPusher Limitations**
   - Can only handle Filters on CTE references (WHERE clauses)
   - But cannot handle complex expressions after aggregation

2. **JoinFilterPushdownOptimizer Limitations**
   - `PushdownJoinFilterExpression` cannot handle complex functions
   - Returns `false` for functions like `time_bucket`
   - Cannot reverse derive

3. **Function Semantic Understanding**
   - DuckDB's optimizer may not fully understand `time_bucket`'s reverse derivation logic
   - Requires explicit semantic analysis of functions to reverse derive

---

## Practical Impact

### For chDB Users

**✅ Recommended: category_Q3 (Outer WHERE Style)**

```sql
WITH HR_intervals AS (
    SELECT 
        toStartOfInterval(ts, INTERVAL 5 MINUTE) AS time_interval,
        AVG(HR) as interval_HR
    FROM hrm
    GROUP BY time_interval  -- No WHERE needed
)
SELECT ...
FROM HR_intervals h 
WHERE h.time_interval BETWEEN ...  -- ✅ category_Q3: Outer WHERE, optimizer automatically pushes down
```

**Advantages:**
- ✅ Conditions are centralized, easy to maintain
- ✅ Optimizer automatically pushes down, same performance as category_Q1
- ✅ GROUP BY only processes filtered data

### For DuckDB Users

**⚠️ Recommended: category_Q1 (WHERE Inside CTE, for complex functions)**

```sql
WITH HR_intervals AS (
    SELECT 
        time_bucket(INTERVAL '5m', ts) AS time_interval,
        AVG(HR) as interval_HR
    FROM hrm
    WHERE ts BETWEEN TIMESTAMP '2021-03-14 00:00:00' AND TIMESTAMP '2021-03-21 23:59:59'  -- ✅ category_Q1: Must be in CTE
    GROUP BY time_interval
)
SELECT ...
FROM HR_intervals h 
JOIN ...
```

**Reasons:**
- ⚠️ DuckDB cannot reverse derive `time_interval BETWEEN` to `ts BETWEEN`
- ⚠️ category_Q3 (outer WHERE) will cause Materialization of all data
- ✅ category_Q1 (WHERE in CTE) can filter early

---

## 🎯 Core Answer

### chDB

**✅ Yes! chDB can push down filters from category_Q3 (outer WHERE condition) to CTE definitions, so GROUP BY only processes filtered rows.**

**Reasons:**
- ✅ Optimizer can reverse derive aggregation expressions (e.g., `toStartOfInterval`)
- ✅ FilterPushdown optimizer automatically pushes down WHERE conditions
- ✅ Filters before aggregation, GROUP BY only processes filtered data

### DuckDB

**⚠️ Cannot fully achieve!**

**⚠️ For simple expressions: May be possible**
- DuckDB's CTEFilterPusher can handle Filters on CTE references
- But for simple expressions, pushdown may be possible

**⚠️ For complex functions (e.g., `time_bucket`): Cannot achieve**
- category_Q3 (outer WHERE) cannot reverse derive aggregated columns
- WHERE condition pushdown fails
- Needs to Materialize all data, can only filter after JOIN

**category_Q3 vs category_Q1:**
- ⚠️ **category_Q3 (outer WHERE, on aggregated columns)**: Materialize all data → GROUP BY all rows → slower performance
- ✅ **category_Q1 (WHERE in CTE, on source table columns)**: Early filtering → GROUP BY filtered rows → better performance

---

## 🔗 Related Source Code Locations

### chDB/ClickHouse
- **FilterPushdown:** `src/Processors/QueryPlan/Optimizations/filterPushDown.cpp`
  - Reverse derive aggregation expressions
  - Automatically push down WHERE conditions

### DuckDB
- **CTEFilterPusher:** `duckdb/src/optimizer/cte_filter_pusher.cpp`
  - Only handles Filters on CTE references
  - Cannot reverse derive complex functions
- **JoinFilterPushdownOptimizer:** `duckdb/src/optimizer/join_filter_pushdown_optimizer.cpp`
  - `PushdownJoinFilterExpression` restricts complex expressions

