# High-performance Embedded​ Data Analytics

- [Dataset (Deprecated)](Dataset.md)
- [Edge Device (Deprecated)](Edge_Device.md)
- [Benchmarks (Deprecated)](Benchmarks.md)

## Dataset Selection

For this project, we chose the [**in-situ Samsung dataset**](https://doi.org/10.6084/m9.figshare.28509740) because it provides **complete raw signals** (not only raw data). The dataset size is about **18 GB**, with per-sensor files:

- `ppg.csv.gz` — Photoplethysmography (PPG), raw input for HR/HRV.
- `hrm.csv.gz` — Heart rate (device-processed from PPG).
- `acc.csv.gz` — 3-axis accelerometer.
- `grv.csv.gz` — Gravity orientation and rotation angle.
- `gyr.csv.gz` — 3-axis gyroscope.
- `lit.csv.gz` — Ambient light intensity.
- `ped.csv.gz` — Pedometer (steps, distance, calories).

---

Since the raw data is very large in size, we split each category of data by `deviceId` and created a separate CSV file for each `deviceId`. The segmented data can be accessed via the following link: [Google Drive](https://drive.google.com/drive/folders/1mVqSyZ9wtxTrftwRxnFE4f0CQtuapSw0)

## Scenarios (queries)

### Reach an agreement

The ideal image of paradigm shift:

- Traditionally: Smart watch sends all data to the server to calculate.
- Embedded: Smart watch calculates the data locally, then sends processed, anonymized data to server.

---

What is the **calculation** we care about?

- It is obvious that the calculation in edge devices not only involves various queries in the database, but also includes the calculation of other programs.
- But in fact, we do not want to design the computations of other programs, but only care about the computations in the database!

Data has **already prepared** in the database's table.

- If it is a real situation, then it is the actual sensors providing real-time production data, which are continuously stored in the database tables (ppg, hrm, acc, grv, gyr, lit, ped tables).
- If we want to conduct a simulation, we can also design an emulator that transmits data to the database at regular intervals.
- However, considering the feasibility and priorities of the current project, it is feasible to directly prepare a database that includes all the data. When using it, only calculations (including aggregations, etc.) within a certain time window can be performed each time.

No need for Server side design and edge - server communication module design:

- In the context of the project, it is true that the edge devices performed a limited amount of computation and then transmitted the results to the server. The purposes of this were to protect privacy and reduce the amount of data transmission.
- However, in this project, we only focus on the database processing part in the edge devices, and we don't even care about the communication between the devices and the servers.

### Sleep Scenario

Could use ppg, hrm, ped, acc, lit tables.

Do aggregation, output: 2 tables.

`sleep_activity_1min` (1-min Aggregation):

| Field           | Type    | Source  | Description                                      | Purpose            |
| --------------- | ------- | ------- | ------------------------------------------------ | ------------------ |
| `deviceId`      | TEXT    | All     | Unique user/device ID                            | Multi-user support |
| `minute_ts`     | BIGINT  | ts      | Minute window start (Unix ms, aligned to minute) | Primary key        |
| `hr_avg`        | REAL    | hrm     | Average heart rate per minute (bpm)              | Physiological load |
| `hr_samples`    | INTEGER | hrm     | Number of HR samples in this minute              | Quality indicator  |
| `activity_mean` | REAL    | acc     | Mean activity intensity (SVM/energy)             | Activity/sleep     |
| `light_avg`     | REAL    | lit     | Average light per minute (lux)                   | Circadian, sleep   |
| `valid_1m`      | TINYINT | Derived | Minute validity: 0=invalid,1=partial,2=valid     | Quality control    |

Sample SQL Queries:

```sql
-- HR per minute
SELECT
  deviceId,
  (ts / 60000) * 60000 AS minute_ts,
  AVG(CAST(HR AS REAL)) AS hr_avg,
  COUNT(*) AS hr_samples
FROM hrm
WHERE HR IS NOT NULL
  AND HR > 10              -- off-wrist
  AND HR BETWEEN 30 AND 220
GROUP BY deviceId, (ts / 60000) * 60000

-- Activity intensity (use SVM magnitude; you can refine to remove gravity if desired)
SELECT
  deviceId,
  (ts / 60000) * 60000 AS minute_ts,
  AVG( SQRT(1.0*x*x + 1.0*y*y + 1.0*z*z) ) AS activity_mean
FROM acc
WHERE x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
GROUP BY deviceId, (ts / 60000) * 60000

-- Light per minute
SELECT
  deviceId,
  (ts / 60000) * 60000 AS minute_ts,
  AVG(CAST(lux AS REAL)) AS light_avg
FROM lit
WHERE lux IS NOT NULL
GROUP BY deviceId, (ts / 60000) * 60000
```

---

`hrv_5min` (5-min HRV Aggregation):

| Field               | Type   | Description                                                                                                                                                                                             | Source (code mapping)                    |
| ------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `deviceId`          | TEXT   | Unique participant/device identifier                                                                                                                                                                    | From input data                          |
| `ts_start`          | BIGINT | Unix epoch timestamp (ms) corresponding to the starting time of the slice/window                                                                                                                        | derived from code logic (slice boundary) |
| `ts_end`            | BIGINT | Unix epoch timestamp (ms) corresponding to the ending time of the slice/window                                                                                                                          | derived from code logic (slice boundary) |
| `missingness_score` | REAL   | HRV analysis missingness ratio:<br>`1 - (observed_ibi / (bpm × T/60))`, where:<br>• `observed_ibi = len(wd["RR_list_cor"])`<br>• `bpm = hrv["bpm"]`<br>• `T` = slice length in seconds (300s for 5-min) | Computed in code → `missingness_ppg`     |
| `HR`                | REAL   | Average heart rate (beats per minute) derived from IBI                                                                                                                                                  | Computed in code → `hrv["bpm"]`          |
| `ibi`               | REAL   | Mean inter-beat interval (ms)                                                                                                                                                                           | Computed in code → `hrv["ibi"]`          |
| `sdnn`              | REAL   | Standard deviation of NN intervals (overall variability)                                                                                                                                                | Computed in code → `hrv["sdnn"]`         |
| `sdsd`              | REAL   | Standard deviation of successive NN interval differences                                                                                                                                                | Computed in code → `hrv["sdsd"]`         |
| `rmssd`             | REAL   | Root mean square of successive NN interval differences (parasympathetic activity marker)                                                                                                                | Computed in code → `hrv["rmssd"]`        |
| `pnn20`             | REAL   | Percentage of successive NN intervals differing by more than 20 ms                                                                                                                                      | Computed in code → `hrv["pnn20"]`        |
| `pnn50`             | REAL   | Percentage of successive NN intervals differing by more than 50 ms                                                                                                                                      | Computed in code → `hrv["pnn50"]`        |
| `lf`                | REAL   | Absolute power of the low-frequency band (0.04–0.15 Hz)                                                                                                                                                 | Computed in code → `hrv["lf"]`           |
| `hf`                | REAL   | Absolute power of the high-frequency band (0.15–0.40 Hz)                                                                                                                                                | Computed in code → `hrv["hf"]`           |
| `lf_hf`             | REAL   | Ratio of LF to HF power                                                                                                                                                                                 | Computed in code → `hrv["lf/hf"]`        |

Findings:

1. The informative HRV metrics (which is the main focus of what we are trying to get from raw data), like statistical features (SDNN, RMSSD, pNN20, pNN50, LF/HF), relay on IBI to be computed.
2. The really important column inter-beat intervals (IBI) CANNOT be calculate using queries alone. IBI is calculated from the signal (PPG), and does go into the realm of biomedical signal processing, which is not the main focus of this particular course. (But technically, it would be feasible to do it on the edge device without a library like [HeartPy](https://python-heart-rate-analysis-toolkit.readthedocs.io/))

### Resting HR anomaly detection using statistics

Idea: if there is abnormaly high heart rate (HR) during resting period, it could correspond to stress, mental unrest or signs of illness. Intervals are compared to a quantile of the person's HR based on sensor hrm when activity is low (steps bellow threshold) and flag it whether it is an anomaly or not. Granuality of the detection can be adjusted.

Tables: hrm, ped
Operations: joins, statistics, aggregation of raw data, CASE/IF

### Fitness index trend analysis

Idea: using a simple measure of fitness index, we can check whether the fitness of the person is increasing or decreasing by fitting a simple regression line. 

Tables: hrm, ped
Operations: joins, division, filtering, aggregation of raw data, regression line

### Categorization of activity

Idea: classifying intervals as either sitting, light or heavy activity by using simple decision rule based method using data from hrm, ped, acc, and lit. 

Tables: hrm, ped, acc, lit
Operations: joins, aggregation of raw data, filtering

## Benchmark Design

### Objectives

This benchmark suite is designed to evaluate different **databases (SQLite, DuckDB, ClickHouse)** on edge devices.

**Stress benchmarking**: apply *amplified workloads* to expose performance differences, scalability, and system headroom, even if such loads exceed normal edge usage.

### Design Principles

- **Relevance:** Scenario workloads mirror actual edge device behavior. Stress workloads deliberately deviate from reality to reveal system limits.
- **Reproducibility:** All experiments must be repeatable with controlled environments.
- **Fairness:** Equivalent logical schemas, equivalent queries, and identical input data are enforced.
- **Verifiability:** Outputs must be validated against a golden reference implementation.
- **Usability:** A kit-based harness provides database adapters, query templates, and reporting tools.

### Stress Workloads (Prototype)

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
  - Top-K: select top-N abnormal windows (e.g., low HR, high activity).
  - Purpose: expose query optimization capabilities, even if rarely used in practice.

### Metrics

- **Performance:** ingestion throughput, query latency (avg, P95, P99), TTFR.
- **Resource usage:** memory peak, CPU utilization, I/O volume.
- **Scalability:** performance degradation with larger data sizes or mixed workloads.
- **Correctness:** query outputs must match golden reference results.

---

**P95 (95th percentile latency):** 95% of requests complete **at or below** this latency; the slowest 5% are worse. It captures typical “tail” performance beyond the average.

**P99 (99th percentile latency):** 99% of requests complete **at or below** this latency; the slowest 1% are worse. This is a stricter tail metric that surfaces rare but impactful slowness.

**TTFR (Time To First Result):** The time from issuing a query until the **first row** (or first page/chunk) is returned to the client. Distinct from total query time; important for interactive/streaming UX.
