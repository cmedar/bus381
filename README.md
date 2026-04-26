# Bus 381 · Live Arrivals & Crossing Detector

> Real-time arrival board and bus crossing tracker for Bucharest line 381 (Tineretului ↔ Piața Română).

---

## What this project does

- **Live departure board** — shows the next bus ETA at every monitored stop, both directions, updated every 90s. Distinguishes GPS-tracked buses from schedule fallback.
- **Crossing detection** — detects when a bus passes each stop by watching for ETA resets and records the estimated arrival time.
- **CSV history** — every ETA reading and every detected crossing is appended to CSV files on the host for later analysis.
- **Kafka backbone** — decouples the poller, crossing detector, and dashboard; enables future replay and stream processing.

---

## How it works

### Stop-as-sensor pattern

There is no direct vehicle tracking. Instead, each stop acts as a sensor:

- The poller asks each stop *"how long until the next 381 arrives?"* every 90 seconds.
- When a stop's ETA drops to near zero, then jumps back up on the next poll, a bus just passed.
- The crossing detector watches for ETA jumps > 60 seconds and fires a crossing event.
- Estimated arrival time = `crossed_at − eta_before` (when we detected the reset, minus how many seconds the bus was away on the last reading).

Accuracy is ±poll interval (±90s worst case). Shorter intervals give tighter estimates.

### Data flow

```
mo-bi.ro API
     │
     ▼
  poller.py  ──────────────────────────────────► arrivals.csv
     │
     ▼  (stb-arrivals topic)
  Kafka
     │
     ├──► crossing_detector.py ──────────────► crossings.csv
     │         │
     │         ▼  (bus-crossings topic)
     │       Kafka
     │         │
     └─────────┴──► dashboard/app.py
```

The dashboard fetches ETAs directly from the API in parallel (for responsiveness) and reads the `bus-crossings` Kafka topic for last arrival times.

---

## Tech stack

| Layer | Technology |
|---|---|
| API source | mo-bi.ro `nextArrivals` endpoint |
| Proxy | Cloudflare Worker (EC2 IPs are blocked by mo-bi.ro) |
| Ingestion | Python · `requests` · `kafka-python` |
| Message bus | Apache Kafka (KRaft mode, no Zookeeper) |
| Persistence | CSV files on EC2 host (host-mounted Docker volume) |
| Dashboard | Streamlit |
| Infrastructure | AWS EC2 t2.small · Docker Compose |

---

## Monitored stops

### Direction 0 — → Piața Română

| seq | Stop | Stop ID |
|---|---|---|
| 0 | Visana | 3688 |
| 1 | Gh. Sincai | 3782 |
| 2 | Bd. Marasesti | 3678 |
| 3 | Piata Sf. Gheorghe | 7257 |
| 4 | Universitate | 7256 |
| 5 | Bd. Nicolae Balcescu | 12353 |
| 6 | Arthur Verona | 12354 |
| 7 | Piata Romana | 6588 |

### Direction 1 — → Tineretului

| seq | Stop | Stop ID |
|---|---|---|
| 0 | Orlando | 5972 |
| 1 | Piata Romana | 3826 |
| 2 | George Enescu | 12514 |
| 3 | Bd. Nicolae Balcescu | 7411 |
| 4 | Piata 21 Dec 1989 | 7462 |
| 5 | Piata Sf. Gheorghe | 6611 |
| 6 | Bd. Marasesti | 3667 |
| 7 | Gh. Sincai | 3784 |

---

## Dashboard

Two stacked tables (one per direction) with columns:

| Column | Description |
|---|---|
| 🟢/🔘 Stop | Stop name · green = GPS live, grey = schedule fallback |
| ETA | Time until next bus (mm ss) |
| Arrives at | Current time + ETA (absolute clock time) |
| Last bus | Estimated time the previous bus passed this stop |

Refreshes every 90 seconds.

---

## CSV files

Written to `/home/bus381/data/` on the EC2 host, mounted into containers at `/data/`.

**`arrivals.csv`** — one row per stop per poll:
```
ingested_at, stop_id, stop_name, direction, corridor_seq, arriving_in_seconds, is_timetable
```

**`crossings.csv`** — one row per detected bus crossing:
```
crossed_at, stop_id, stop_name, direction, corridor_seq, eta_before, eta_after
```

---

## Project structure

```
bus381/
├── ingestion/
│   ├── config.py               # stop IDs, Kafka topics, poll interval
│   ├── poller.py               # polls all stops, publishes to Kafka + arrivals.csv
│   └── crossing_detector.py    # detects crossings, publishes to Kafka + crossings.csv
├── dashboard/
│   └── app.py                  # Streamlit live departure board
├── docker-compose.yml          # Kafka + poller + crossing-detector + dashboard
└── Dockerfile
```

---

## Deployment

The project runs on EC2 via Docker Compose. GitHub Actions deploys on push to `main`.

```bash
# on EC2 — manual deploy
docker compose pull
docker compose up -d --build
```

The data directory is created automatically on first run:
```
/home/bus381/data/
├── arrivals.csv
└── crossings.csv
```

---

## Roadmap

### Phase 1 — Live ingestion ✅ (current)
- Poll mo-bi.ro API every 90s for all corridor stops, both directions
- Publish ETA readings to Kafka (`stb-arrivals`)
- Detect bus crossings via ETA reset pattern, publish to Kafka (`bus-crossings`)
- Live dashboard: ETA, arrival time, last bus per stop
- Persist all readings and crossings to CSV

### Phase 2 — Historical analysis (next)
- Load `arrivals.csv` and `crossings.csv` into Databricks
- Build Silver table: clean crossing events with travel times between stops
- Build Gold table: average journey time per stop pair, per hour, per day of week
- Identify peak congestion windows and structural delays

### Phase 3 — Baseline ETA (future)
- Feed historical baseline back into dashboard
- Show predicted arrival with confidence interval based on accumulated data
- Self-improving: the more data collected, the tighter the interval

---

*Data sourced from [mo-bi.ro](https://mo-bi.ro) — Mobilitate în București.*
