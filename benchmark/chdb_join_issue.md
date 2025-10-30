# chDB Query Performance Difference

## 1. Query Syntax Comparison

### Query 1: BETWEEN in JOIN ON Clause

```sql
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
    h.time_interval AS time_interval, 
    h.interval_HR, 
    a.acc_magnitude, 
    g.gyr_magnitude,
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
    AND h.time_interval BETWEEN toDateTime('2021-03-14 00:00:00') AND toDateTime('2021-03-21 23:59:59')  ⚠️ Key Difference
JOIN GYR_intervals g ON h.time_interval = g.time_interval
ORDER BY h.time_interval;
```

**Syntax Features:**

- ✅ JOIN condition includes equality: `h.time_interval = a.time_interval`
- ✅ JOIN condition also includes filter: `h.time_interval BETWEEN ... AND ...`
- ✅ BETWEEN condition is part of the ON clause

### Query 2: BETWEEN in WHERE Clause

```sql
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
    h.time_interval AS time_interval, 
    h.interval_HR, 
    a.acc_magnitude, 
    g.gyr_magnitude,
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
JOIN ACC_intervals a ON h.time_interval = a.time_interval  ✅ Equality condition only
JOIN GYR_intervals g ON h.time_interval = g.time_interval
WHERE h.time_interval BETWEEN toDateTime('2021-03-14 00:00:00') AND toDateTime('2021-03-21 23:59:59')  ⚠️ Key Difference
ORDER BY h.time_interval;
```

**Syntax Features:**

- ✅ JOIN condition only has equality: `h.time_interval = a.time_interval`
- ✅ Filter condition in WHERE clause: `WHERE h.time_interval BETWEEN ... AND ...`
- ✅ JOIN condition and filter condition are separated

### Syntax Difference Summary Table

| Feature                       | Query 1                                                                  | Query 2                              |
| ----------------------------- | ------------------------------------------------------------------------ | ------------------------------------ |
| **First JOIN Condition**      | `h.time_interval = a.time_interval`<br>`AND h.time_interval BETWEEN ...` | `h.time_interval = a.time_interval`  |
| **Filter Condition Position** | ON clause (part of JOIN condition)                                       | WHERE clause (independent)           |
| **Second JOIN Condition**     | `h.time_interval = g.time_interval`                                      | `h.time_interval = g.time_interval`  |
| **JOIN Condition Complexity** | Complex condition (equality + range)                                     | Simple condition (equality only)     |
| **Semantics**                 | BETWEEN is a semantic part of JOIN                                       | BETWEEN is a result filter condition |

## 2. Execution Plan Analysis

### 2.1 Query 1 Execution Plan (Actual Execution Plan)

```bash
Expression ((Project names + Projection))
  Aggregating
    Expression (Before GROUP BY)
      Expression
        Join
          Expression
            Aggregating
              Filter (( + (Before GROUP BY + Change column names to column identifiers)))
                ReadFromMergeTree (sensor.hrm)
          Expression
            Aggregating
              Filter (( + (Before GROUP BY + Change column names to column identifiers)))
                ReadFromMergeTree (sensor.acc)
```

**Execution Plan Features:**

- Filter step after Aggregating, before Join
- JOIN operation has two input streams (left and right tables)
- Filter appears on both branches (both left and right tables have Filter)
- **Note**: Although Filter is before JOIN, the BETWEEN in the ON clause will still be evaluated as residual_condition during JOIN

### 2.2 Query 2 Execution Plan (Actual Execution Plan)

```bash
Expression ((Project names + Projection))
  Aggregating
    Expression ((Before GROUP BY + ))
      Expression
        Join
          Expression
            Aggregating
              Filter (( + (Before GROUP BY + Change column names to column identifiers)))
                ReadFromMergeTree (sensor.hrm)
          Expression
            Aggregating
              Filter (( + (Before GROUP BY + Change column names to column identifiers)))
                ReadFromMergeTree (sensor.acc)
```

**Execution Plan Features:**

- Filter step also after Aggregating, before Join
- WHERE condition is pushed down to the same Filter position as Query 1
- JOIN operation only handles simple equality conditions (no residual_condition)
- **Key Difference**: Although Filter position is the same, the JOIN expression tree is different

### 2.3 Pipeline Execution Plan Comparison

#### Query 1 Pipeline (Actual Execution Plan)

```bash
(Expression)
ExpressionTransform × 10
  (Aggregating)
  Resize 10 → 10
    AggregatingTransform × 10
      (Expression)
      ExpressionTransform × 10
        (Expression)
        ExpressionTransform × 10
          (Join)  ⚠️ JOIN operation with complex condition
          SimpleSquashingTransform × 10
            ColumnPermuteTransform × 10  ⚠️ Extra column permutation (due to complex condition)
              JoiningTransform × 10 2 → 1
                Resize 10 → 10
                  (Expression)
                  ExpressionTransform × 10
                    (Aggregating)
                    Resize 9 → 10
                      AggregatingTransform × 9
                        (Filter)  ⚠️ Filter after Aggregating
                        FilterTransform × 18
                          (ReadFromMergeTree)
                          MergeTreeSelect(pool: ReadPool, algorithm: Thread) × 9 0 → 1
                  (Expression)
                  Resize × 2 10 → 1
                    FillingRightJoinSide
                      SimpleSquashingTransform
                        FillingRightJoinSide
                          SimpleSquashingTransform
                            FillingRightJoinSide
```

**Pipeline Features:**

- `ColumnPermuteTransform`: Extra column permutation operation (because JOIN contains complex condition)
- Filter at the same position in execution plan (after Aggregating)
- JOIN needs to handle residual_condition

#### Query 2 Pipeline (Actual Execution Plan)

```bash
(Expression)
ExpressionTransform × 10
  (Aggregating)
  Resize 10 → 10
    AggregatingTransform × 10
      (Expression)
      ExpressionTransform × 10
        (Expression)
        ExpressionTransform × 10
          (Join)  ✅ JOIN operation, equality condition only
          SimpleSquashingTransform × 10
            JoiningTransform × 10 2 → 1  ✅ No ColumnPermuteTransform needed
              Resize 10 → 10
                (Expression)
                ExpressionTransform × 10
                  (Aggregating)
                  Resize 9 → 10
                    AggregatingTransform × 9
                      (Filter)  ✅ Filter after Aggregating (WHERE push-down)
                      FilterTransform × 18
                        (ReadFromMergeTree)
                        MergeTreeSelect(pool: ReadPool, algorithm: Thread) × 9 0 → 1
                (Expression)
                Resize × 2 10 → 1
                  FillingRightJoinSide
                    SimpleSquashingTransform
                      FillingRightJoinSide
                        SimpleSquashingTransform
                          FillingRightJoinSide
                            SimpleSquashingTransform
```

**Pipeline Features:**

- **No** `ColumnPermuteTransform` (JOIN condition is simple, no column permutation needed)
- Filter at the same position in execution plan (WHERE push-down)
- JOIN only needs to handle simple equality conditions

### 2.4 Execution Plan Difference Analysis

| Dimension                     | Query 1                                                         | Query 2                                   |
| ----------------------------- | --------------------------------------------------------------- | ----------------------------------------- |
| **Filter Position**           | After Aggregating, before Join                                  | After Aggregating, before Join (same)     |
| **JOIN Condition Complexity** | Complex condition (equality + BETWEEN residual_condition)       | Simple condition (equality only)          |
| **Pipeline Difference**       | Contains `ColumnPermuteTransform`                               | Does not contain `ColumnPermuteTransform` |
| **BETWEEN Evaluation Count**  | 2 times (Filter filtering + JOIN residual_condition evaluation) | 1 time (only in Filter step)              |
| **JOIN Expression**           | Contains `mixed_join_expression` (BETWEEN)                      | No extra expression                       |

## 3. Source Code Deep Dive

### 3.1 WHERE Condition Push-Down Mechanism

**Source Code Location:** `src/Processors/QueryPlan/Optimizations/filterPushDown.cpp`

#### Key Function: `tryPushDownFilter`

```cpp
// Lines 472-700
size_t tryPushDownFilter(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, 
                         const Optimization::ExtraSettings & /*settings*/)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();
    auto * filter = typeid_cast<FilterStep *>(parent.get());
    
    if (!filter)
        return 0;

    // ... other checks ...

    // Key: Try to push down Filter to JOIN
    if (auto updated_steps = tryPushDownOverJoinStep(parent_node, nodes, child))
        return updated_steps;

    // ... other push-down logic ...
}
```

#### Key Function: `tryPushDownOverJoinStep`

```cpp
// Lines 223-470
static size_t tryPushDownOverJoinStep(QueryPlan::Node * parent_node, 
                                      QueryPlan::Nodes & nodes, 
                                      QueryPlanStepPtr & child)
{
    auto * filter = assert_cast<FilterStep *>(parent.get());
    
    // Identify JOIN step
    auto * logical_join = typeid_cast<JoinStepLogical *>(child.get());
    auto * join = typeid_cast<JoinStep *>(child.get());
    
    if (!join && !logical_join)
        return 0;

    // Get JOIN's left and right input streams
    const auto & left_stream_input_header = child->getInputHeaders().front();
    const auto & right_stream_input_header = child->getInputHeaders().back();

    // Key: Analyze Filter expression to identify parts that can be pushed down
    auto join_filter_push_down_actions = 
        filter->getExpression().splitActionsForJOINFilterPushDown(
            filter->getFilterColumnName(),
            filter->removesFilterColumn(),
            left_stream_available_columns_to_push_down,  // Left table available columns
            *left_stream_input_header,
            right_stream_available_columns_to_push_down, // Right table available columns
            *right_stream_input_header,
            equivalent_columns_to_push_down,             // Equivalent columns
            equivalent_left_stream_column_to_right_stream_column,
            equivalent_right_stream_column_to_left_stream_column);

    // If Filter can be pushed down to left stream
    if (join_filter_push_down_actions.left_stream_filter_to_push_down)
    {
        // Add Filter after Aggregating in left stream
        updated_steps += addNewFilterStepOrThrow(
            parent_node, nodes,
            {std::move(*join_filter_push_down_actions.left_stream_filter_to_push_down), 
             0, 
             join_filter_push_down_actions.left_stream_filter_removes_filter},
            0 /*child_idx*/,
            false /*update_parent_filter*/);
    }

    // If Filter can be pushed down to right stream
    if (join_filter_push_down_actions.right_stream_filter_to_push_down && allow_push_down_to_right)
    {
        // Add Filter after Aggregating in right stream
        updated_steps += addNewFilterStepOrThrow(
            parent_node, nodes,
            {std::move(*join_filter_push_down_actions.right_stream_filter_to_push_down), 
             0, 
             join_filter_push_down_actions.right_stream_filter_removes_filter},
            1 /*child_idx*/,
            false /*update_parent_filter*/);
    }

    return updated_steps;
}
```

**Key Mechanism:**

1. **Analyze Filter Expression**: `splitActionsForJOINFilterPushDown` splits the WHERE condition's ActionsDAG
2. **Determine Push-Down Position**: Identify which columns belong to left stream, which to right stream
3. **Add Filter Step**: Add Filter step before JOIN (after Aggregating)

### 3.2 BETWEEN Processing in ON Clause

**Source Code Location:** `src/Processors/QueryPlan/JoinStepLogical.cpp`

#### Key Function: `addJoinConditionToTableJoin`

```cpp
// Lines 402-457
bool addJoinConditionToTableJoin(JoinCondition & join_condition, 
                                  TableJoin::JoinOnClause & table_join_clause,
                                  JoinExpressionActions & expression_actions,
                                  JoinPlanningContext join_context)
{
    std::vector<JoinPredicate> new_predicates;
    
    for (size_t i = 0; i < join_condition.predicates.size(); ++i)
    {
        auto & predicate = join_condition.predicates[i];
        
        // Type conversion handling
        predicateOperandsToCommonType(predicate, expression_actions, join_context);
        
        // Key: Distinguish between equality and non-equality conditions
        if (PredicateOperator::Equals == predicate.op 
         || PredicateOperator::NullSafeEquals == predicate.op)
        {
            // Equality condition → JOIN key (for building hash table)
            auto [left_key_node, right_key_node] = leftAndRightNodes(predicate);
            bool null_safe_comparison = PredicateOperator::NullSafeEquals == predicate.op;
            
            table_join_clause.addKey(
                predicate.left_node.getColumnName(), 
                predicate.right_node.getColumnName(), 
                null_safe_comparison);
            
            new_predicates.push_back(predicate);
        }
        else if (join_context.is_asof)
        {
            // ASOF predicate special handling
            new_predicates.push_back(predicate);
        }
        else
        {
            // ⚠️ Key: Non-equality conditions (like BETWEEN) moved to residual_conditions
            auto predicate_action = predicateToCondition(
                predicate, 
                expression_actions.post_join_actions);
            join_condition.residual_conditions.push_back(predicate_action);
            // Note: new_predicates does not include this condition, as it's not a JOIN key
        }
    }

    join_condition.predicates = std::move(new_predicates);
    return !join_condition.predicates.empty();
}
```

**Key Mechanism:**

1. **Equality Condition** (like `=`): Becomes JOIN key, used to build hash table
2. **Non-Equality Condition** (like `BETWEEN`): Moved to `residual_conditions` (residual condition)
3. **residual_conditions** need to be evaluated additionally during JOIN matching

#### Key Function: `convertToPhysical` - residual_condition Processing

```cpp
// Lines 555-772
JoinPtr JoinStepLogical::convertToPhysical(...)
{
    // ... JOIN algorithm selection logic ...
    
    // Key: Process residual_conditions
    JoinActionRef residual_filter_condition(nullptr);
    if (join_expression.disjunctive_conditions.empty())
    {
        // Merge all residual_conditions (including BETWEEN)
        residual_filter_condition = concatMergeConditions(
            join_expression.condition.residual_conditions, 
            expression_actions.post_join_actions);
    }
    
    // Check if residual_condition can be pushed down
    if (residual_filter_condition && canPushDownFromOn(join_info))
    {
        // When can be pushed down, use as post_filter
        post_filter = residual_filter_condition;
    }
    else if (residual_filter_condition)
    {
        // ⚠️ When cannot be pushed down, must evaluate in JOIN expression
        ActionsDAG dag;
        if (is_explain_logical)
        {
            dag = expression_actions.post_join_actions->clone();
        }
        else
        {
            dag = std::move(*expression_actions.post_join_actions);
            *expression_actions.post_join_actions = ActionsDAG(dag.getRequiredColumns());
        }
        
        // Extract residual_condition as mixed_join_expression
        auto & outputs = dag.getOutputs();
        for (const auto * node : outputs)
        {
            if (node->result_name == residual_filter_condition.getColumnName())
            {
                outputs = {node};
                break;
            }
        }
        
        // ⚠️ Key: residual_condition becomes JOIN's mixed_join_expression
        // This means it must be evaluated on every JOIN match
        ExpressionActionsPtr & mixed_join_expression = table_join->getMixedJoinExpression();
        mixed_join_expression = std::make_shared<ExpressionActions>(std::move(dag), actions_settings);
    }
    
    // ... subsequent JOIN algorithm selection ...
}
```

**Key Mechanism:**

1. **residual_condition Processing**: BETWEEN is collected as residual_condition
2. **Push-Down Check**: `canPushDownFromOn` determines if it can be pushed down
3. **mixed_join_expression**: If cannot be pushed down, BETWEEN becomes part of the JOIN expression and must be evaluated on every match

#### Key Function: `canPushDownFromOn`

```cpp
// Lines 320-332
bool canPushDownFromOn(const JoinInfo & join_info, std::optional<JoinTableSide> side = {})
{
    // Only specific types of JOIN can push down residual_condition
    bool is_suitable_kind = join_info.kind == JoinKind::Inner
        || join_info.kind == JoinKind::Cross
        || join_info.kind == JoinKind::Comma
        || join_info.kind == JoinKind::Paste
        || (side == JoinTableSide::Left && join_info.kind == JoinKind::Right)
        || (side == JoinTableSide::Right && join_info.kind == JoinKind::Left);

    return is_suitable_kind
        && join_info.expression.disjunctive_conditions.empty()
        && join_info.strictness == JoinStrictness::All;
}
```

**For Query 1's case:**

- ✅ JOIN type is Inner (can push down)
- ✅ No disjunctive_conditions
- ✅ strictness is All
- **However**: Even though it can be pushed down, because BETWEEN is in the ON clause, it still needs to be evaluated as a semantic part of the JOIN expression

### 3.3 Execution Flow Comparison

#### Query 1 Execution Flow (Source Code Level)

```bash
1. Parsing Phase (Planner)
   ├─ ON clause parsing
   │   ├─ Equality condition → JOIN key
   │   └─ BETWEEN condition → residual_condition
   └─ Generate JoinStepLogical

2. Optimization Phase (QueryPlanOptimizations)
   ├─ Filter push-down optimization (if Filter exists)
   │   └─ May add extra Filter steps
   └─ JOIN logical optimization

3. Physical Conversion Phase (convertToPhysical)
   ├─ Process residual_condition
   │   ├─ If canPushDownFromOn → post_filter
   │   └─ Otherwise → mixed_join_expression ⚠️
   └─ Select JOIN algorithm (HashJoin/MergeJoin, etc.)

4. Execution Phase (QueryPipeline)
   ├─ Filter step (if optimizer added)
   ├─ Aggregating step
   └─ JOIN step
       ├─ Build hash table (based on JOIN keys)
       └─ Probe matching
           ├─ Hash lookup (based on JOIN keys)
           └─ Evaluate mixed_join_expression ⚠️ (contains BETWEEN)
              → Must evaluate on every match!
```

#### Query 2 Execution Flow (Source Code Level)

```bash
1. Parsing Phase (Planner)
   ├─ JOIN condition: Equality condition only
   ├─ WHERE condition: Independent filter condition
   └─ Generate JoinStepLogical + FilterStep

2. Optimization Phase (QueryPlanOptimizations)
   ├─ Filter push-down optimization (tryPushDownFilter)
   │   ├─ Analyze WHERE condition
   │   ├─ splitActionsForJOINFilterPushDown
   │   └─ Add Filter step before JOIN ✅
   └─ JOIN logical optimization

3. Physical Conversion Phase (convertToPhysical)
   ├─ JOIN condition: Equality condition only (simple)
   ├─ No residual_condition ✅
   └─ Select JOIN algorithm (pure hash JOIN)

4. Execution Phase (QueryPipeline)
   ├─ Filter step (WHERE condition pushed down here) ✅
   ├─ Aggregating step
   └─ JOIN step
       ├─ Build hash table (based on JOIN keys)
       └─ Probe matching
           └─ Hash lookup (that's all) ✅
              → No additional evaluation needed!
```

### 3.4 Source Code Key Differences Summary

| Difference Point               | Query 1                                    | Query 2                               |
| ------------------------------ | ------------------------------------------ | ------------------------------------- |
| **Condition Parsing Location** | `addJoinConditionToTableJoin`              | WHERE condition not in JOIN condition |
| **BETWEEN Processing**         | → `residual_condition`                     | → Independent Filter step             |
| **residual_condition**         | Exists, needs evaluation                   | Does not exist                        |
| **JOIN Expression**            | Contains `mixed_join_expression` (BETWEEN) | None, only JOIN keys                  |
| **Runtime Evaluation**         | Evaluate BETWEEN on every match            | No evaluation needed                  |

## 4. Performance Test Results

### 4.1 Test Environment

- **Data Source**: vs14_data_chdb (sensor data)
- **HRM Table**: 1,495,193 records
- **ACC Table**: 7,573,354 records
- **GYR Table**: 7,544,931 records
- **chDB Version**: 3.6.0
- **Test Platform**: macOS ARM64

### 4.2 Performance Test Results

#### Full Three-Table JOIN Test (5-run average)

| Query       | Average Time | Fastest Time | Slowest Time | Standard Deviation |
| ----------- | ------------ | ------------ | ------------ | ------------------ |
| **Query 1** | 68.5 ms      | 67.7 ms      | 68.9 ms      | 0.4 ms             |
| **Query 2** | 43.9 ms      | 35.6 ms      | 73.9 ms      | 15.0 ms            |

Performance Difference: Query 2 is 55.81% faster

### 4.4 Performance Difference Cause Analysis

**Key Differences:**

1. **JOIN Algorithm Complexity**: Query 1 needs to evaluate complex conditions, Query 2 only needs hash matching
2. **residual_condition Overhead**: Query 1 must evaluate BETWEEN on every match (even if already filtered)
3. **Hash Table Construction**: Query 1's hash key calculation may be more complex

## 5. Summary and Best Practices

### 5.1 Core Findings

1. **WHERE Condition Push-Down is Very Effective**
   - WHERE conditions are automatically pushed down by the optimizer before JOIN
   - Position is the same as ON clause Filter

2. **JOIN Condition Complexity is Key**
   - JOIN with simple equality conditions is much faster than complex conditions
   - residual_condition needs additional evaluation, even if data is already filtered

3. **Non-Equality Conditions in ON Clause Become residual_condition**
   - Even if they can be pushed down, they still need to be evaluated as part of JOIN semantics

### 5.2 Performance Optimization Recommendations

#### Recommended Pattern (INNER JOIN)

```sql
-- Clear separation of JOIN condition and filter condition
FROM HR_intervals h 
JOIN ACC_intervals a ON h.time_interval = a.time_interval  -- Simple equality condition
JOIN GYR_intervals g ON h.time_interval = g.time_interval
WHERE h.time_interval BETWEEN '2021-03-14' AND '2021-03-21'  -- Filter condition independent

Advantages:
✓ Faster execution (tested 55.81% faster)
✓ Simple JOIN condition, hash JOIN is efficient
✓ Good code readability
✓ Easy for optimizer to optimize
```

#### ❌ Not Recommended Pattern

```sql
-- Mixing JOIN condition and filter condition
FROM HR_intervals h 
JOIN ACC_intervals a 
  ON h.time_interval = a.time_interval 
  AND h.time_interval BETWEEN '2021-03-14' AND '2021-03-21'  -- Complex condition

Disadvantages:
✗ Slower execution (tested 55.81% slower)
✗ Complex JOIN condition, needs residual_condition evaluation
✗ Hash table construction and probing both slow down
✗ Poor code readability
```

### 5.4 Key Lessons

1. **Trust the Optimizer**: Modern query optimizers automatically push down WHERE conditions, no manual optimization needed
2. **Keep JOIN Conditions Simple**: JOIN with pure equality conditions is much faster than complex conditions
3. **Separate Concerns**: Separate JOIN logic and filter logic for clearer code and better performance
4. **Theory Needs Verification**: Theoretical analysis must be combined with actual testing, optimizer behavior may exceed intuition

## Appendix: Related Source Code Locations

### Filter Push-Down Optimization

- `src/Processors/QueryPlan/Optimizations/filterPushDown.cpp`
  - `tryPushDownFilter` (Line 472)
  - `tryPushDownOverJoinStep` (Line 223)
  - `splitActionsForJOINFilterPushDown` (Line 367)

### JOIN Condition Parsing

- `src/Processors/QueryPlan/JoinStepLogical.cpp`
  - `addJoinConditionToTableJoin` (Line 402)
  - `canPushDownFromOn` (Line 320)
  - `convertToPhysical` (Line 555)
  - `residual_condition` processing (Lines 665-734)

### Optimizer Execution Order

- `src/Processors/QueryPlan/Optimizations/Optimizations.h`
  - Optimization list (Lines 108-124)
  - `tryPushDownFilter` is the 6th optimization

### Optimizer Main Flow

- `src/Processors/QueryPlan/Optimizations/optimizeTree.cpp`
  - `optimizeTreeFirstPass` (Line 24)
  - `optimizeTreeSecondPass` (Line 123)
