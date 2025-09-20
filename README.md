# dataset analytics

- [Dataset](Dataset.md)
- [Edge Device](Edge_Device.md)

## Dataset Selection

This project focuses on multimodal sensor data collected from wearable devices (smartwatches). After evaluating publicly available Apple Watch datasets on Kaggle, we found several limitations:

- Missing heart rate data during sleep;
- Lack of raw sensor signals (e.g., accelerometer), as most data had already been pre-processed by the watch.

Therefore, we shifted to open datasets that provide **raw sensor data**. The final choice was:

- **In-situ wearable-based dataset of continuous heart rate variability monitoring accompanied by sleep diaries** (Samsung Galaxy Active 2, with raw PPG, HR, accelerometer, and light data).

This dataset is around **18 GB** and contains the following sensor files:

- `ppg.csv.gz` — Photoplethysmography (PPG) signals.
- `hrm.csv.gz` — Heart rate (likely PPG-derived via smartwatch algorithms).
- `acc.csv.gz` — 3-axis accelerometer.
- `grv.csv.gz` — Gravity orientation and rotation angle.
- `gyr.csv.gz` — 3-axis gyroscope.
- `lit.csv.gz` — Ambient light intensity.
- `ped.csv.gz` — Pedometer (steps, distance, calories).

For this project, the **core data sources** are PPG and HRM (for heart rate and HRV), accelerometer (for activity and sleep detection), and light (for circadian analysis).

## Project Scenario

We simulate a **hybrid system** consisting of an **edge device** (the smartwatch) and a **server**:

- **Edge device (on-watch):**

  - Continuously collects sensor data.
  - Performs lightweight aggregation:

    - **Per minute:** average heart rate, activity intensity, light → stored in `sleep_activity_1min`.
    - **Per 5 minutes:** HRV metrics extracted from PPG via [HeartPy](https://python-heart-rate-analysis-toolkit.readthedocs.io/) → stored in `hrv_5min`.
  - Uses a lightweight database (SQLite / DuckDB) for compact storage and efficient incremental uploads.

- **Server (research/analytics side):**

  - Receives aggregated rows from the edge device.
  - Performs advanced HRV analytics (nightly medians, pNN20/pNN50 distributions).
  - Computes sleep metrics (sleep onset, wake time, WASO, efficiency).
  - Produces research-ready tables and dashboards.

This design is particularly suited for **sleep monitoring scenarios**: the edge device handles **real-time aggregation with low latency**, while the server runs **computationally heavy analyses**.

## Benchmark Design

The benchmark aims to evaluate the **database and aggregation performance on the edge device**, not the HRV algorithm itself.

### Test Scenarios

- **Insertion stress test**: continuous ingestion of 49 participants’ multi-sensor data (≈18 GB preprocessed and replayed).
- **Aggregation latency test**: measure the delay from raw sensor streams to completed `sleep_activity_1min` and `hrv_5min` rows.

### Key Metrics

- **Throughput**: ingestion speed (rows/sec).
- **Latency**: time to complete aggregation for each time window.
- **Memory usage**: peak RAM consumption during aggregation.
- **CPU utilization**: processor load under continuous ingestion and aggregation.

### Notes

- No complex query stress testing, since the edge device mainly performs aggregation and uploads, not interactive analytics.
- HRV algorithm (HeartPy) benchmarking is not the focus; only real-time feasibility within a 5-minute window is required.

## Tools

### Data preprocessing & simulation

- **Python (pandas, numpy)**: split and clean the 18 GB dataset into per-user streams.
- **DuckDB / SQLite**: lightweight storage for sensor subsets and aggregation results.
- **Kafka / Redpanda** (optional): simulate sensor data streams and replay workloads for ingestion benchmarks.

### Edge aggregation & analysis

- **HeartPy**: PPG-based HRV feature extraction for 5-minute windows.
- **SQL (SQLite/DuckDB compatible)**: implement time window aggregation, incremental sync, and storage.

### Server-side analytics

- **Parquet/Arrow**: efficient batch uploads and storage.
- **Python (statsmodels, scipy)**: advanced HRV computations, nightly medians, and sleep metrics.
- **Visualization tools**: Grafana or matplotlib for performance monitoring and data visualization.
