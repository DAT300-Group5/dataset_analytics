# High-performance Embedded​ Data Analytics

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

Since the raw data is very large in size, we split each category of data by `deviceId` and created a separate CSV file for each `deviceId`. The segmented data can be accessed via the following link: [Google Drive](https://drive.google.com/drive/folders/1mVqSyZ9wtxTrftwRxnFE4f0CQtuapSw0), or you can download the raw data and run `python split_data.py` yourself.

## Benchmark

![structure](./pic/benchmark_structure.png)

More detailed Doc: [benchmark/README.md](benchmark/README.md)

## Scenarios (queries)

### Reach an agreement

The ideal image of paradigm shift:

- Traditionally: Smart watch sends all data to the server to calculate.
- Embedded: Smart watch calculates the data locally, then sends processed, anonymized data to server.

---

What is the **calculation** we care about?

- It is obvious that the calculation in edge devices not only involves various queries in the database, but also includes the calculation of other programs.
- But in fact, we do not want to design the computations of other programs, but only care about the computations in the database!

---

![structure](./pic/structure.png)

The figure above is an example of the entrie system design. **BUT** we **DON'T** need to do all the things.

Data has **already prepared** in the database's table.

- If it is a real situation, then it is the actual sensors providing real-time production data, which are continuously stored in the database tables (ppg, hrm, acc, grv, gyr, lit, ped tables).
- If we want to conduct a simulation, we can also design an emulator that transmits data to the database at regular intervals.
- However, considering the feasibility and priorities of the current project, it is feasible to directly prepare a database that includes all the data. When using it, only calculations (including aggregations, etc.) within a certain time window can be performed each time.

No need for Server side design and edge - server communication module design:

- In the context of the project, it is true that the edge devices performed a limited amount of computation and then transmitted the results to the server. The purposes of this were to protect privacy and reduce the amount of data transmission.
- However, in this project, we only focus on the database processing part in the edge devices, and we don't even care about the communication between the devices and the servers.

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

### Sleep Scenario - HRV Aggregation

Findings:

1. The informative HRV metrics (which is the main focus of what we are trying to get from raw data), like statistical features (SDNN, RMSSD, pNN20, pNN50, LF/HF), relay on IBI to be computed.
2. The really important column inter-beat intervals (IBI) CANNOT be calculate using queries alone. IBI is calculated from the signal (PPG), and does go into the realm of biomedical signal processing, which is not the main focus of this particular course. (But technically, it would be feasible to do it on the edge device without a library like [HeartPy](https://python-heart-rate-analysis-toolkit.readthedocs.io/))

Could be future work.
