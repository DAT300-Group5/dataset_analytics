# Edge Device

## 1. Input Tables

- **ppg**
  - `deviceId TEXT`
  - `ts BIGINT` (Unix ms)
  - `ppg REAL`

- **hrm**
  - `deviceId TEXT`
  - `ts BIGINT`
  - `HR REAL` (Heart rate, bpm)

- **acc**
  - `deviceId TEXT`
  - `ts BIGINT`
  - `x REAL, y REAL, z REAL` (3-axis accelerometer)

- **lit**
  - `deviceId TEXT`
  - `ts BIGINT`
  - `lux REAL` (Ambient light intensity)

## 2. Database

Two lightweight tables are maintained on the device:

- **sleep_activity_1min** → Focused on **behavior & environment** (HR, activity, light, validity).  
- **hrv_5min** → Focused on **autonomic recovery** (core HRV metrics).  

This separation allows compact on-device storage and flexible joins on the server.

### Table: `sleep_activity_1min` (1-min Aggregation)

| Field           | Type    | Source  | Description                                      | Purpose            |
| --------------- | ------- | ------- | ------------------------------------------------ | ------------------ |
| `deviceId`      | TEXT    | All     | Unique user/device ID                            | Multi-user support |
| `minute_ts`     | BIGINT  | ts      | Minute window start (Unix ms, aligned to minute) | Primary key        |
| `hr_avg`        | REAL    | hrm     | Average heart rate per minute (bpm)              | Physiological load |
| `hr_samples`    | INTEGER | hrm     | Number of HR samples in this minute              | Quality indicator  |
| `activity_mean` | REAL    | acc     | Mean activity intensity (SVM/energy)             | Activity/sleep     |
| `light_avg`     | REAL    | lit     | Average light per minute (lux)                   | Circadian, sleep   |
| `valid_1m`      | TINYINT | Derived | Minute validity: 0=invalid,1=partial,2=valid     | Quality control    |

The **Communication Module** periodically queries and uploads data in bulk.

### Table: `hrv_5min` (5-min HRV Aggregation)

Many fields are calculated by [HeartPy](https://python-heart-rate-analysis-toolkit.readthedocs.io/).

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

**Feature**: One row per 5 min, focusing only on HRV metrics and quality indicators.

## 3. Example SQL (SQLite/DuckDB Compatible)

### 1-min Aggregation (`sleep_activity_1min`)

```sql
-- HR per minute
SELECT
  deviceId,
  ts - (ts % 60000) AS minute_ts,
  AVG(HR) AS hr_avg,
  COUNT(*) AS hr_samples
FROM hrm
WHERE HR BETWEEN 30 AND 220
GROUP BY 1,2;

-- Activity intensity (use SVM magnitude; you can refine to remove gravity if desired)
SELECT
  deviceId,
  ts - (ts % 60000) AS minute_ts,
  AVG(sqrt(x*x + y*y + z*z)) AS activity_mean
FROM acc
GROUP BY 1,2;

-- Light per minute
SELECT
  deviceId,
  ts - (ts % 60000) AS minute_ts,
  AVG(lux) AS light_avg
FROM lit
GROUP BY 1,2;
```

### 5-min HRV Aggregation (`hrv_5min`)

**Important:** All HRV metrics come from the HeartPy pipeline (code variables shown on the table). SQL only handles **time windowing** and **inserts**; the metrics are computed in code.

#### Derive 5-min window boundaries via SQL (for convenience)

```sql
SELECT
  deviceId,
  ts - (ts % 300000) AS ts_start,
  ts - (ts % 300000) + 300000 AS ts_end
FROM ppg
GROUP BY 1,2,3;
```

#### Insert rows after HeartPy computation (code → SQL)

```sql
INSERT INTO hrv_5min (
  deviceId, ts_start, ts_end,
  missingness_score, HR, ibi, sdnn, sdsd, rmssd, pnn20, pnn50, lf, hf, lf_hf
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
```

> Tip: `ts_start` should be `min(ts)` of the slice/window and `ts_end = ts_start + 300000` for 5-min windows; if your slice is shorter (e.g., edge gaps), use the actual `max(ts)` you processed.

## 4. Edge Queries

The **Communication Module** is responsible for periodic uploads (e.g., every 30 minutes).  

### Pull new minute-level rows

```sql
SELECT *
FROM sleep_activity_1min
WHERE deviceId = ?
  AND minute_ts > ?
ORDER BY minute_ts
LIMIT 2000;
```

### Pull new 5-min HRV rows

```sql
SELECT *
FROM hrv_5min
WHERE deviceId = ?
  AND ts_end > ?
ORDER BY ts_end
LIMIT 1000;
```

> The server maintains per-device checkpoints (`last_uploaded_ts`, `last_uploaded_ts_end`).  
> The Communication Module executes these queries and uploads results at fixed intervals.

## 5. Validity Rules (Post-hoc Filtering)

`hrv_5min` exposes **one quality metric**: `missingness_score` (PPG-based, equals your code’s `missingness_ppg`). Define validity purely on this (optionally combine with HR plausibility and motion via join).

### Core rule (based on `missingness_score`)

- **Valid**: `missingness_score ≤ 0.30`
- **Partial**: `0.30 < missingness_score ≤ 0.70`
- **Invalid**: `missingness_score > 0.70`

> Rationale: lower missingness = better coverage.
