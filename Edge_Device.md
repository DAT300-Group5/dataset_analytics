# Edge Device

## 1. Input Tables (Cleaned)

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
| `synced`        | BOOLEAN | System  | Whether uploaded to server                       | Incremental upload |

**Feature**: One row per minute, compact and lightweight, suitable for quick queries and later joins.

### Table: `hrv_5min` (5-min HRV Aggregation)

| Field          | Type    | Source  | Description                                     | Purpose            |
| -------------- | ------- | ------- | ----------------------------------------------- | ------------------ |
| `deviceId`     | TEXT    | hrm/ppg | Unique user/device ID                           | Multi-user support |
| `window_start` | BIGINT  | ts      | 5-min window start (Unix ms, aligned to 5 min)  | Primary key        |
| `hr_mean`      | REAL    | hrm/ppg | Mean HR in the window (bpm)                     | Physiological load |
| `hr_median`    | REAL    | hrm/ppg | Median HR in the window (bpm)                   | Robust HR measure  |
| `sdnn`         | REAL    | hrm/ppg | Standard deviation of NN intervals              | Total variability  |
| `rmssd`        | REAL    | hrm/ppg | Root mean square of successive NN differences   | Vagal activity     |
| `sdsd`         | REAL    | hrm/ppg | Standard deviation of successive NN differences | Beat-to-beat var.  |
| `pnn20`        | REAL    | hrm/ppg | Proportion of                                   | ΔNN                |
| `pnn50`        | REAL    | hrm/ppg | Proportion of                                   | ΔNN                |
| `nn_count`     | INTEGER | hrm/ppg | Number of valid NN in window                    | Statistical base   |
| `valid_ratio`  | REAL    | Derived | Ratio of valid duration / 300 s                 | Coverage           |
| `valid_5m`     | TINYINT | Derived | Window validity: 0=invalid,1=partial,2=valid    | Quality control    |
| `source`       | TINYINT | Derived | IBI source: 0=PPG, 1=HRM approximation          | Precision mark     |
| `synced`       | BOOLEAN | System  | Whether uploaded to server                      | Incremental upload |

**Feature**: One row per 5 min, focusing only on HRV metrics and quality indicators. Context (activity, light, etc.) can be joined from `sleep_activity_1min` when needed.

## 3. Example SQL (SQLite/DuckDB Compatible)

### 1-min Aggregation

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

-- Activity intensity
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

### 5-min HRV Aggregation (device-side)

> Note: `SDNN`, `RMSSD`, `SDSD`, `pNNx` should be computed in code (Welford/online algorithm), then inserted into `hrv_5min`. SQLite lacks built-in stddev functions.

```sql
-- Example insert after computing in code
INSERT INTO hrv_5min (
  deviceId, window_start, hr_mean, hr_median, sdnn, rmssd, sdsd, pnn20, pnn50,
  nn_count, valid_ratio, valid_5m, source, synced
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,0);
```

## 4. Edge Queries (Incremental Upload)

```sql
-- Fetch unsynced new minute-level data
SELECT *
FROM sleep_activity_1min
WHERE synced = 0 AND deviceId = ? AND minute_ts > ?
ORDER BY minute_ts
LIMIT 2000;

-- Fetch unsynced new 5-min HRV data
SELECT *
FROM hrv_5min
WHERE synced = 0 AND deviceId = ? AND window_start > ?
ORDER BY window_start
LIMIT 1000;

-- Mark as synced (wrap in transaction)
UPDATE sleep_activity_1min
SET synced = 1
WHERE deviceId = ? AND minute_ts <= ?;

UPDATE hrv_5min
SET synced = 1
WHERE deviceId = ? AND window_start <= ?;
```

## 5. Validity Rules

- **Minute validity (`valid_1m`)**
  - 2 (valid): sufficient NN, low activity, normal sensor quality.
  - 1 (partial): some NN but low coverage or mild motion/noise.
  - 0 (invalid): no NN or strong motion/artifact.

- **5-min validity (`valid_5m`)**
  - Compute `valid_ratio = effective duration / 300 s`.
  - Recommended thresholds:
    - ≥0.70 and low motion → 2 (valid)
    - 0.30–0.70 → 1 (partial)
    - <0.30 → 0 (invalid)

## 6. Indexing & Performance Recommendations

- **SQLite**: Create indexes on time/device columns for fast incremental queries.
  - `CREATE INDEX idx_act_min_ts ON sleep_activity_1min(deviceId, minute_ts);`
  - `CREATE INDEX idx_hrv5m_ts ON hrv_5min(deviceId, window_start);`
- **Design principle**: Append-only tables, lightweight aggregation on device, complex analysis on server.
