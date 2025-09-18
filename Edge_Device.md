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

- **grv**
  - `deviceId TEXT`
  - `ts BIGINT`
  - `x REAL, y REAL, z REAL, w REAL` (Gravity orientation + quaternion)

- **lit**
  - `deviceId TEXT`
  - `ts BIGINT`
  - `lux REAL` (Ambient light intensity)

## 2. Database

- **minute_summary** → Focus on **behavior & environment** (HR, activity, light, posture).  
- **hrv_5min** → Focus on **autonomic recovery** (basic HRV metrics).  

Together, they support **lightweight edge filtering + deeper cloud analysis**.

### Table: `minute_summary` (1-min Aggregation)

| Field                | Type     | Source       | Description                                      | Purpose            |
|----------------------|----------|--------------|--------------------------------------------------|--------------------|
| `deviceId`           | TEXT     | All sensors  | Unique user/device ID                            | Multi-user support |
| `minute_ts`          | BIGINT   | ts           | Minute window start (Unix ms, aligned to minute) | Primary key        |
| `hr_avg`             | REAL     | hrm          | Average heart rate per minute (bpm)              | Physiological load |
| `activity_intensity` | REAL     | acc          | Activity intensity (SVM mean)                    | Activity/sleep     |
| `light_avg`          | REAL     | lit          | Average light per minute (lux)                   | Circadian, sleep   |
| `posture`            | SMALLINT | grv          | Posture classification: 0=unknown, 1=lying, 2=sitting, 3=standing | Posture/sleep |
| `validity`           | TINYINT  | Derived      | Data validity: 0=ok, 1=partial, 2=bad            | Quality control    |
| `synced`             | BOOLEAN  | System flag  | Whether uploaded to server                       | Incremental upload |

**Feature**: One row per minute, compact fields, suitable for quick queries (e.g., night inactivity, HR variation).

### Table: `hrv_5min` (5-min HRV Aggregation)

| Field            | Type    | Source        | Description                                                 | Purpose              |
|------------------|---------|---------------|-------------------------------------------------------------|----------------------|
| `deviceId`       | TEXT    | hrm / ppg     | Unique user/device ID                                       | Multi-user support   |
| `window_start`   | BIGINT  | ts            | 5-min window start (Unix ms, aligned to 5 min)              | Primary key          |
| `rmssd`          | REAL    | hrm/ppg → IBI | RMSSD, reflects vagal activity                              | Autonomic recovery   |
| `sdnn`           | REAL    | hrm/ppg → IBI | SDNN, overall HRV level                                     | Total variability    |
| `nn_count`       | INTEGER | hrm/ppg → IBI | Number of valid IBI                                         | Statistical base     |
| `coverage_ratio` | REAL    | hrm/ppg → IBI | Data coverage (valid beats / 300s)                          | Quality assessment   |
| `validity`       | TINYINT | Derived       | Validity: 0=ok, 1=partial, 2=bad                            | Quality control      |
| `is_approx`      | BOOLEAN | Source flag   | TRUE=IBI from HRM (approx), FALSE=IBI from PPG (true)       | Precision mark       |
| `synced`         | BOOLEAN | System flag   | Whether uploaded to server                                  | Incremental upload   |

**Feature**: One row per 5 min, provides basic HRV summary, lightweight for device, extendable at server (e.g., pNN20/pNN50).

## 3. Example SQL (DuckDB)

### 1-min Aggregation

```sql
-- HR per minute
CREATE OR REPLACE TABLE minute_hr AS
SELECT
  deviceId,
  ts - (ts % 60000) AS minute_ts,
  AVG(HR) AS hr_avg,
  COUNT(*) AS hr_samples
FROM hrm
WHERE HR BETWEEN 30 AND 220
GROUP BY 1,2;

-- ACC activity intensity
CREATE OR REPLACE TABLE minute_activity AS
SELECT
  deviceId,
  ts - (ts % 60000) AS minute_ts,
  AVG(sqrt(x*x + y*y + z*z)) AS activity_intensity,
  COUNT(*) AS acc_samples
FROM acc
GROUP BY 1,2;

-- LIT average per minute
CREATE OR REPLACE TABLE minute_light AS
SELECT
  deviceId,
  ts - (ts % 60000) AS minute_ts,
  AVG(lux) AS light_avg,
  COUNT(*) AS lit_samples
FROM lit
GROUP BY 1,2;

-- GRV posture classification
CREATE OR REPLACE TABLE minute_posture AS
WITH g AS (
  SELECT
    deviceId,
    ts - (ts % 60000) AS minute_ts,
    AVG(ABS(z)) AS gz_abs_avg
  FROM grv
  GROUP BY 1,2
)
SELECT
  deviceId,
  minute_ts,
  CASE
    WHEN gz_abs_avg >= 0.8 THEN 1  -- lying
    WHEN gz_abs_avg <= 0.2 THEN 3  -- standing
    ELSE 2                         -- sitting
  END AS posture
FROM g;

-- Merge all minute-level features
CREATE OR REPLACE TABLE minute_summary AS
SELECT
  COALESCE(h.deviceId, a.deviceId, l.deviceId, p.deviceId) AS deviceId,
  COALESCE(h.minute_ts, a.minute_ts, l.minute_ts, p.minute_ts) AS minute_ts,
  h.hr_avg,
  a.activity_intensity,
  l.light_avg,
  p.posture,
  CASE
    WHEN COALESCE(h.hr_samples,0) + COALESCE(a.acc_samples,0) + COALESCE(l.lit_samples,0) >= 5 THEN 0
    WHEN COALESCE(h.hr_samples,0) + COALESCE(a.acc_samples,0) + COALESCE(l.lit_samples,0) BETWEEN 1 AND 4 THEN 1
    ELSE 2
  END AS validity,
  FALSE AS synced
FROM minute_hr h
FULL OUTER JOIN minute_activity a
  ON h.deviceId=a.deviceId AND h.minute_ts=a.minute_ts
FULL OUTER JOIN minute_light l
  ON COALESCE(h.deviceId,a.deviceId)=l.deviceId AND COALESCE(h.minute_ts,a.minute_ts)=l.minute_ts
FULL OUTER JOIN minute_posture p
  ON COALESCE(h.deviceId,a.deviceId,l.deviceId)=p.deviceId
 AND COALESCE(h.minute_ts,a.minute_ts,l.minute_ts)=p.minute_ts;
```

### 5-min HRV Aggregation

**Goal**: Generate lightweight 5-min HRV summaries on device, then upload to server.

**Principles**:

- Device computes **RMSSD / SDNN** + coverage.
- Choice of input:
  - **Low-resource**: HRM → approximate IBI.
  - **High-resource**: PPG → true IBI via peak detection.
- Server recomputes advanced HRV (pNN20, pNN50, distributions).

#### Step 1. Extract IBI

**Option A (approx, from HRM):**

```sql
CREATE OR REPLACE TABLE ibi_from_hrm AS
SELECT
  deviceId,
  ts,
  60000.0 / HR AS ibi_ms
FROM hrm
WHERE HR BETWEEN 30 AND 220;
```

#### Step 2. Aggregate into 5-min windows

```sql
CREATE OR REPLACE TABLE hrv_5min AS
WITH w AS (
  SELECT
    deviceId,
    ts - (ts % 300000) AS window_start,
    ts,
    ibi_ms,
    ibi_ms - LAG(ibi_ms) OVER (
      PARTITION BY deviceId, ts - (ts % 300000)
      ORDER BY ts
    ) AS d_ibi
  FROM ibi_from_hrm
),
agg AS (
  SELECT
    deviceId,
    window_start,
    STDDEV_SAMP(ibi_ms) AS sdnn,
    SQRT(AVG(d_ibi*d_ibi)) FILTER (WHERE d_ibi IS NOT NULL) AS rmssd,
    COUNT(ibi_ms) AS nn_count,
    LEAST(1.0, COUNT(ibi_ms) * (AVG(ibi_ms)/1000.0) / 300.0) AS coverage_ratio
  FROM w
  GROUP BY 1,2
)
SELECT
  deviceId,
  window_start,
  rmssd,
  sdnn,
  nn_count,
  coverage_ratio,
  CASE
    WHEN coverage_ratio >= 0.7 AND nn_count >= 30 THEN 0
    WHEN coverage_ratio >= 0.3 THEN 1
    ELSE 2
  END AS validity,
  TRUE AS is_approx,   -- from HRM (approx)
  FALSE AS synced
FROM agg;
```

## 4. Edge Queries (Simulation)

> **Goal**: Perform only **lightweight and predictable** queries on the device; leave complex analysis to the server.

### Uploading Only “New Data” (Incremental Upstream)

#### `minute_summary`

```sql
-- Fetch unsynced new minute-level data
SELECT *
FROM minute_summary
WHERE synced = FALSE
  AND deviceId = ?
  AND minute_ts > ?
ORDER BY minute_ts
LIMIT 2000;
```

#### `hrv_5min`

```sql
SELECT *
FROM hrv_5min
WHERE synced = FALSE
  AND deviceId = ?
  AND window_start > ?
ORDER BY window_start
LIMIT 1000;
```

#### Mark as Synced

```sql
UPDATE minute_summary
SET synced = TRUE
WHERE deviceId = ?
  AND minute_ts <= ?;

UPDATE hrv_5min
SET synced = TRUE
WHERE deviceId = ?
  AND window_start <= ?;
```

> **SQLite note**: Syntax is the same; wrap batch updates in a **transaction** to reduce I/O overhead.

### Indexing & Performance Recommendations

- **DuckDB**: Columnar compression and vectorization are sufficient; design both summary tables as **append-only** for stability.
- **SQLite**: Create indexes on the following columns to ensure query speed:
  - `CREATE INDEX idx_min_dev_ts ON minute_summary(deviceId, minute_ts);`
  - `CREATE INDEX idx_hrv_dev_ts ON hrv_5min(deviceId, window_start);`
- Always prefer **range conditions + ordered primary keys** to avoid full table scans.
- **Read/Write separation**: Schedule aggregation writes and queries at different times; before upload, run `SELECT` followed by `UPDATE synced` in the **same transaction**.
