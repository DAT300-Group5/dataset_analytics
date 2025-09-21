# Benchmark Design

## 1. Objectives

This benchmark suite is designed to evaluate different combinations of **databases (e.g., SQLite, DuckDB, ClickHouse)** and **data formats (CSV, Parquet, Apache Arrow, native storage engines)**, also queries (but same outcome) on edge devices.

Two complementary objectives are defined:

1. **Scenario-based benchmarking** – validate whether systems can handle the *realistic low-pressure workloads* of single-device edge environments.
2. **Stress benchmarking** – apply *amplified workloads* to expose performance differences, scalability, and system headroom, even if such loads exceed normal edge usage.

## 2. Design Principles

- **Relevance:** Scenario workloads mirror actual edge device behavior. Stress workloads deliberately deviate from reality to reveal system limits.
- **Reproducibility:** All experiments must be repeatable with controlled environments.
- **Fairness:** Equivalent logical schemas, equivalent queries, and identical input data are enforced.
- **Verifiability:** Outputs must be validated against a golden reference implementation.
- **Usability:** A kit-based harness provides database adapters, query templates, and reporting tools.

## 3. Workload Models

### 3.1 Scenario Workloads (Core, Low Pressure)

- **E2E Ingestion + Aggregation**
  - Input: continuous sensor data streams (e.g., heart rate, accelerometer, PPG).
  - Processing:
    - Aggregate into `sleep_activity_1min` (1-minute summaries).
    - Aggregate into `hrv_5min` (5-minute HRV summaries).
  - Metrics: ingestion throughput, end-to-end aggregation latency, peak memory, CPU usage, energy per row (if supported).
- **E2E Upload**
  - Test the efficiency of different data formats when transferred over the network: JSON, Arrow IPC/Flight.
  - Metrics:
    - Throughput (MB/s)
    - CPU utilization
    - Network traffic volume (bytes transferred)
    - Serialization/deserialization overhead
- **Periodic Queries**
  - Operation: communication module fetches new rows every 30 minutes.
  - Queries:
    - Q1: Fetch unsynced rows from `sleep_activity_1min`.
    - Q2: Fetch unsynced rows from `hrv_5min`.
  - Metrics: query latency (avg, P95, P99), time-to-first-row (TTFR), resource usage.

### 3.2 Stress Workloads (Comparative, High Pressure)

- **Aggregate More**
  - Aggregate across multiple days of data (e.g., 24h or 7d) instead of only 1min / 5min windows.
  - Purpose: test scan and aggregation under large input sizes.
- **Frequent Queries**
  - Increase query frequency dramatically (e.g., query every minute instead of every 30 minutes).
  - Purpose: stress the system under frequent access patterns, even if such frequency exceeds typical edge usage.
- **Concurrent Workloads**
  - Mix ingestion and query workloads concurrently (e.g., while ingesting 1-minute windows, also execute range queries).
  - Purpose: test system stability and isolation.
- **Complex Queries (example)**
  - Joins: `sleep_activity_1min` ⟷ `hrv_5min` aligned on time windows.
  - Top-K: select top-N abnormal windows (e.g., low HRV, high activity).
  - Purpose: expose query optimization capabilities, even if rarely used in practice.

### 3.3 Optional Microbenchmarks (Diagnostic Only)

- **Scan-only:** measure raw reading performance (CSV vs Parquet vs Arrow).
- **Operator tests:** group-by, window, sorting performance in isolation.
- **Write-only:** measure sustained insertion performance with different storage backends.

## 4. Metrics

- **Performance:** ingestion throughput, query latency (avg, P95, P99), TTFR.
- **Resource usage:** memory peak, CPU utilization, I/O volume.
- **Energy efficiency:** joules per row (optional, hardware-dependent).
- **Scalability:** performance degradation with larger data sizes or mixed workloads.
- **Correctness:** query outputs must match golden reference results.

## 5. Fairness Rules

- **Unified logical schema:**
  - `sleep_activity_1min(deviceId, minute_ts, hr_avg, activity_mean, light_avg, …)`
  - `hrv_5min(deviceId, window_start, rmssd, sdnn, missingness_score, …)`
- **Allowed optimizations:** indexes, compression, vectorization, database-native tuning.
- **Prohibited optimizations:**
  - Pre-aggregation beyond 1-minute / 5-minute granularity.
  - Semantic deviations (e.g., different window alignment).
- **Environment control:** identical hardware, fixed power mode, steady-state operation.
- **Repetition:** minimum 5 runs per workload, reporting mean and variance.

## 6. Reporting Format

1. **Environment:** device type, CPU, memory, storage, OS, DB version, tuning.
2. **Workload specification:** scale factor, replay speed, query interval, stress level.
3. **Configuration:** indexes, partitions, compression, WAL settings.
4. **Results:** throughput, latency distributions, resource usage, energy (if available).
5. **Correctness check:** pass/fail validation against golden outputs.
6. **Fair use policy:** results must be presented with complete context; cherry-picking or selective metrics is not permitted.

## Q&A

### Q1. Why does the benchmark include both *scenario workloads* and *stress workloads*?

**A:** Scenario workloads represent the **real edge environment**: single-device, low pressure, periodic aggregation and retrieval. They ensure that systems are **usable and efficient under actual conditions**. Stress workloads, on the other hand, deliberately **exceed real-world pressure** (larger data, concurrent queries, complex joins) to **expose differences and scalability limits**. Without stress testing, many systems appear equally “fast enough,” making comparison meaningless. The combination of both provides a balanced evaluation: scenario benchmarks check practical viability, stress benchmarks reveal performance headroom.

### Q2. If stress workloads do not match reality, why are they necessary?

**A:** Stress workloads are not about simulating today’s reality, but about:

1. **Amplifying differences** between systems for fair comparison.
2. **Exploring future potential** if device demands increase or hardware improves.
3. **Identifying bottlenecks** early (e.g., a database that collapses under heavier load is riskier long-term).
    Thus, stress workloads complement, rather than replace, scenario benchmarks.

### Q3. What if one system performs worse under stress but is good enough in scenarios?

**A:** Scenario benchmarks are the **hard requirement**. If a system passes all scenario tests, it is acceptable for deployment. Stress results then guide **strategic decisions**:

- If future workloads might grow, prefer the system with better stress resilience.
- If stress weaknesses are irrelevant to expected use (e.g., complex joins not needed), they can be ignored.
- In some cases, a **hybrid strategy** is viable (e.g., SQLite for lightweight queries, DuckDB for batch analysis).


### Q4. Why is correctness validation required? Isn’t performance enough?

**A:** Performance without correctness is meaningless. A system that returns “fast but wrong” results cannot be trusted. Golden outputs ensure all systems compute **semantically identical results** (same windowing, HRV scores, missingness thresholds). This prevents unfair advantages from semantic shortcuts.


### Q5. Why include microbenchmarks if they are not real workloads?

**A:** Microbenchmarks act as **diagnostic tools**. They isolate components like scanning (CSV vs Parquet), operators (group-by, sort), or write paths. When end-to-end results differ, microbenchmarks help explain whether the cause is file parsing, query execution, or storage engine behavior. They are not decision criteria but improve interpretability.

### Q6. Can benchmark results alone decide which database to adopt?

**A:** Not always. Benchmark results are one **dimension** of decision-making. Other factors include:

- **Energy efficiency** (critical for edge).
- **Deployment footprint** (binary size, dependencies).
- **Maintainability** (community support, ease of updates).
- **Integration cost** (API compatibility, ecosystem).
   Benchmarking provides **quantitative evidence**, but final decisions require broader engineering considerations.

### Q7. How should results be reported to ensure fairness?

**A:**

- All results must include **environment details** (hardware, OS, DB version).
- Metrics should include both **throughput and latency** (avg, P95, P99).
- Graphs must use **common scales**; cherry-picking or selective metrics is disallowed.
- Correctness validation must be reported alongside performance.