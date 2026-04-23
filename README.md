# Bus 381 Tineretului–Română Transit

> **How late is the next 381 — really?**
> A Spark Structured Streaming pipeline that ingests live STB vehicle positions, sessionizes each run along the Tineretului–Piața Română corridor, builds a self-improving historical baseline, and serves a live ETA with a confidence signal.

---

## What this project does

The STB app shows you where bus 381 is. This pipeline goes further — it tells you when the next 381 is estimated to arrive at Gh. Șincai station, and how long it will take to reach Piața Română at this specific hour, estimated from historical data accumulated for this exact time interval.

It does this by treating every vehicle ping as a stream event, building stateful sessions per run, and comparing live pace against an accumulated baseline that grows richer every day.

**On day 1** the confidence interval is wide — the system has little history.  
**By day 14** the baseline has absorbed rush hours, weekday patterns, and typical congestion pockets — the ETA narrows.

---

## Architecture

> *See `architecture.png` for the full pipeline diagram.*

The pipeline runs in four layers:

```
STB API (info.stbsa.ro)
    │  20s poll · line 381 vehicles endpoint
    ▼
Python poller  ──►  Kafka topic: stb-vehicles
                         │
                         ▼
          ┌──────────────────────────────┐
          │  Spark Structured Streaming  │
          │  1. Parse + filter           │
          │     · Line 381 only          │
          │     · Corridor bounding box  │
          │  2. Watermark + window       │
          │     · 2 min late tolerance   │
          │     · 5 min rolling window   │
          │  3. Sessionize               │
          │     · Per-vehicle run state  │
          │     · Deviation from baseline│
          └──────────────────────────────┘
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
       Bronze          Silver          Gold
    Raw pings      Sessions +      Baseline +
                   deviation       ETA signal
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
       Live ETA      Delay          Baseline
       + confidence  heatmap        trend
          └──────────────────────────────┘
               Streamlit · 30s refresh
```

---

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.11 · `requests` · `kafka-python` |
| Message bus | Apache Kafka (local Docker) or Confluent Cloud free tier |
| Stream processing | Apache Spark 3.5 · Structured Streaming · `mapGroupsWithState` |
| Storage | Delta Lake · medallion architecture (Bronze / Silver / Gold) |
| Orchestration | Databricks Community Edition Workflows |
| Dashboard | Streamlit · `pydeck` for map · `plotly` for charts |
| Infrastructure | AWS S3 (or DBFS) for Delta tables |

---

## The corridor

**Line 381** · Colegiul Național Gh. Șincai → Piața Română  
6 stops · ~21 min scheduled · every 7 min peak

```
Colegiul Gh. Șincai  ──►  Clăbucet  ──►  ...  ──►  Piața Română
     [start]                                           [end]
  44.4281°N, 26.0966°E                          44.4436°N, 26.0977°E
```

The bounding box filter keeps only pings within this corridor, discarding the rest of the line's route. This makes processing cheap and keeps the baseline clean.

---

## Data model

### Bronze — raw pings
```
vehicle_id      STRING     STB vehicle identifier
license_plate   STRING
latitude        DOUBLE
longitude       DOUBLE
line_id         INT        381
direction_id    INT
passenger_count INT
ingested_at     TIMESTAMP  wall-clock time of poll
event_time      TIMESTAMP  derived from ping sequence
```

### Silver — sessions
```
session_id          STRING     vehicle_id + run start timestamp
vehicle_id          STRING
run_start_ts        TIMESTAMP  first ping inside corridor
run_end_ts          TIMESTAMP  last ping inside corridor (null if in progress)
duration_seconds    INT        null if in progress
stop_sequence       ARRAY<INT> stops visited in order
pace_kmh            DOUBLE     average speed across corridor
deviation_seconds   INT        vs current baseline (positive = late)
is_complete         BOOLEAN
```

### Gold — baseline + ETA signal
```
hour_of_day         INT
day_of_week         INT
baseline_duration_s DOUBLE     rolling mean of completed sessions
baseline_stddev_s   DOUBLE     rolling stddev — drives confidence interval
sample_count        INT        sessions used in baseline
eta_next_arrival    TIMESTAMP  projected arrival at Piața Română
confidence_low_s    INT        baseline_duration - 1.5 * stddev
confidence_high_s   INT        baseline_duration + 1.5 * stddev
updated_at          TIMESTAMP
```

---

## Spark concepts demonstrated

| Concept | Where |
|---|---|
| `readStream` from Kafka | Ingestion into Bronze |
| Watermarking | 2-minute late-arrival tolerance on event_time |
| Sliding window aggregation | 5-minute windows for pace computation |
| `mapGroupsWithState` | Stateful sessionization per vehicle run |
| `foreachBatch` write | Upsert sessions into Silver Delta table |
| Incremental Gold update | Micro-batch aggregation Bronze → Gold |
| Trigger interval | `processingTime="20 seconds"` micro-batch |

This combination — stateful arbitrary state management via `mapGroupsWithState` on top of watermarked windows — is the core Spark Structured Streaming showcase. It goes beyond windowed aggregations into true stateful session tracking where state persists across micro-batches.

---

## Baseline growth mechanic

The Gold table is append-friendly. Every completed session contributes to the rolling mean and stddev for its `(hour_of_day, day_of_week)` bucket.

```
Week 1  →  sparse buckets, wide confidence intervals (±8 min)
Week 2  →  weekday rush hours filling in, intervals tightening
Week 4  →  full week coverage, intervals stable (±2 min typical)
```

This self-improving behaviour is visible on the dashboard's baseline trend panel — the confidence band visibly narrows as data accumulates.

---

## Dashboard panels

**Live ETA** — next expected arrival at Piața Română for line 381, with a confidence band derived from Gold. Updates every 30 seconds. Color-coded: green (on pace), amber (1–3 min late), red (3+ min late).

**Delay heatmap** — hour-of-day (x) × day-of-week (y) grid. Cell color = median deviation in seconds. Reveals when the corridor is structurally slow vs when it's an outlier.

**Baseline trend** — rolling chart of session durations vs baseline mean. Shows how today's runs compare to accumulated history. The confidence band is drawn as a shaded region.

---

## Project structure

```
tineretului-romana/
├── ingestion/
│   ├── poller.py           # polls STB API, publishes to Kafka
│   └── config.py           # line ID, corridor bbox, poll interval
├── streaming/
│   ├── pipeline.py         # main Spark Structured Streaming job
│   ├── sessionizer.py      # mapGroupsWithState logic
│   └── schema.py           # Bronze / Silver / Gold schemas
├── storage/
│   └── delta_utils.py      # table init, upsert helpers
├── dashboard/
│   └── app.py              # Streamlit app, three panels
├── notebooks/
│   └── exploration.ipynb   # ad-hoc analysis, baseline inspection
├── architecture.png        # pipeline diagram
└── README.md
```

---

## Running locally

### Prerequisites
- Python 3.11+
- Docker (for Kafka) or Confluent Cloud free account
- Databricks Community Edition (for Spark + Delta Lake)
- AWS S3 bucket or DBFS path for Delta tables

### 1. Start Kafka
```bash
docker-compose up -d   # starts Zookeeper + Kafka broker
```

### 2. Configure
```bash
cp config.example.env .env
# set STB_LINE_ID, KAFKA_BOOTSTRAP, S3_BUCKET / DBFS_PATH
```

### 3. Start the poller
```bash
python ingestion/poller.py
```

### 4. Run the Spark job
```bash
# on Databricks Community Edition
# upload streaming/pipeline.py and run as a Workflow
# or run locally with spark-submit if Spark installed
spark-submit streaming/pipeline.py
```

### 5. Launch the dashboard
```bash
streamlit run dashboard/app.py
```

---

## MVP scope and future additions

**MVP (this repo)**
- Single line (381), single direction (→ Piața Română)
- Vehicle position stream only
- Self-building baseline from live data
- Three-panel Streamlit dashboard

**Future**
- Weather stream enrichment (cross-stream join) — delay by condition
- Both directions on line 381
- Additional lines on the same corridor
- Anomaly detection — flag sessions that break the pattern
- GTFS schedule join — deviation vs published timetable, not just historical pace

---

## Why this project

This project demonstrates the streaming half of the modern data engineering stack: continuous ingestion, stateful micro-batch processing, and a baseline that evolves in real time — covering the Spark Structured Streaming depth that cloud data roles are hiring for.

---

*Built on data from [STB SA](https://info.stbsa.ro) — Societatea de Transport București.*
