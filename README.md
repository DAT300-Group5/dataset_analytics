# High-performance Embedded​ Data Analytics

- [Dataset](Dataset.md)
- [Edge Device](Edge_Device.md)
- [Benchmarks](Benchmarks.md)

## Dataset Selection

This project focuses on **multimodal sensor data collected from wearable devices (smartwatches)**.

For this project, we chose the [**in-situ Samsung dataset**](https://springernature.figshare.com/articles/dataset/In-situ_wearable-based_dataset_of_continuous_heart_rate_variability_monitoring_accompanied_by_sleep_diaries/28509740) because it provides **complete raw signals**. The dataset size is about **18 GB**, with per-sensor files:

- `ppg.csv.gz` — Photoplethysmography (PPG), raw input for HR/HRV.
- `hrm.csv.gz` — Heart rate (device-processed from PPG).
- `acc.csv.gz` — 3-axis accelerometer.
- `grv.csv.gz` — Gravity orientation and rotation angle.
- `gyr.csv.gz` — 3-axis gyroscope.
- `lit.csv.gz` — Ambient light intensity.
- `ped.csv.gz` — Pedometer (steps, distance, calories).

**Core sources for this project:**

- **PPG + HRM** → heart rate & HRV
- **Accelerometer** → activity & sleep detection
- **Light** → circadian rhythm analysis

## Project Scenario

We simulate a **hybrid edge–server system**:

- **Edge device (watch):**

  - Continuously collects sensor signals.
  - Performs **lightweight aggregation**:

    - **Per minute:** HR average, activity, light → stored in `sleep_activity_1min`.
    - **Per 5 minutes:** HRV metrics (RMSSD, SDNN, etc.) from PPG via [HeartPy](https://python-heart-rate-analysis-toolkit.readthedocs.io/) → stored in `hrv_5min`.
  - Stores results in **SQLite/DuckDB** for compactness and efficient incremental uploads.

- **Server (analytics side):**

  - Ingests aggregated rows from the watch.
  - Runs **advanced HRV analytics** (nightly medians, pNN20/pNN50 distributions).
  - Computes **sleep metrics**: sleep onset, wake time, WASO, efficiency.
  - Produces research-ready datasets and dashboards.

This design is well-suited for **sleep monitoring**:

- Edge device = **real-time, low-latency aggregation**.
- Server = **computationally heavy, retrospective analysis**.

## Benchmark Design

The benchmark framework is derived from `Benchmarks.md`:

- **Scenario-based workloads** → validate realistic low-pressure, single-device behavior.
- **Stress workloads** → deliberately push beyond reality to reveal performance headroom.

### Scenario workloads

- **Ingestion + Aggregation:** PPG/ACC streams aggregated into `sleep_activity_1min` and `hrv_5min`.
- **Upload efficiency:** compare JSON vs Arrow Flight, ...
- **Periodic queries:** fetch new rows (every 30 min).

### Stress workloads

- **Aggregate more:** daily/weekly windows.
- **Frequent queries:** every minute.
- **Concurrent workloads:** mix ingestion + queries.
- **Complex queries:** joins & top-K selection.

Metrics include throughput, latency (avg, P95, P99), CPU/memory use, and correctness validation against golden outputs.

## Tools

- **Preprocessing & simulation:**

  - Python (pandas, numpy) to split/restructure the dataset.
  - Kafka/Redpanda to simulate real-time sensor streams.

- **Edge aggregation & storage:**

  - HeartPy (PPG→HRV).
  - SQLite/DuckDB (incremental sync + compact storage).

- **Server-side analytics:**

  - Parquet/Arrow for uploads.
  - Python (scipy, statsmodels) for advanced HRV/sleep metrics.
  - Grafana/matplotlib for visualization.

