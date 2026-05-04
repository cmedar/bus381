# Bus 381 · Live Arrivals & Corridor Tracker

> Real-time arrival board and journey tracker for Bucharest line 381 (Tineretului ↔ Piața Română).

![Dashboard snapshot](snapshot.png)

---

## What this project does

- **Live departure board** — next bus ETA at every monitored stop, both directions, updated every 10s. Distinguishes GPS-tracked buses from schedule fallback.
- **Crossing detection** — detects when a bus passes each stop by watching for ETA resets; records estimated arrival time.
- **Journey sessionization** — groups per-stop crossings into complete bus journeys (Gh. Sincai → Piața Română), reconstructing how long each bus took at each segment.
- **Corridor stats** — avg / best / worst corridor time based on last 5 completed journeys, shown live in the dashboard.
- **Kafka backbone** — decouples poller, crossing detector, session tracker, and dashboard.

---

## Dashboard

![Dashboard snapshot](snapshot.png)

Each direction section shows:

- **Corridor summary** — avg (last 5 buses), best, worst, total journeys tracked
- **Stop rows** — ETA countdown · absolute arrival time · last bus time · 🟢 live GPS / 🔘 schedule
- **Journey matrix** (collapsed) — last 10 completed buses A–J, elapsed time from Gh. Sincai at each stop

Refreshes every 10 seconds. API calls are cached 45s so the refresh is free.

---

## How it works

### Stop-as-sensor pattern

There is no direct vehicle tracking. Each stop acts as a sensor:

1. The poller asks each stop *"how long until the next 381 arrives?"* every 45 seconds.
2. When a stop's ETA drops to near zero then jumps back up, a bus just passed.
3. The crossing detector fires when ETA jumps > 60 seconds, publishing a `bus-crossings` event.
4. Estimated arrival time = `crossed_at − eta_before`.

**Adaptive fast-polling:** when ETA at Gh. Sincai drops below 60s, the poller switches to 20s intervals (10s when ETA = 0) until the bus resets to ≥ 120s. This tightens crossing detection at the corridor entry point.

### FIFO sessionization

The session tracker assigns each crossing to the oldest in-progress bus at the previous stop (FIFO). A plausibility guard rejects assignments where the gap since the last crossing exceeds 15 minutes, preventing mismatch errors when multiple buses are close together.

### Data flow

```
mo-bi.ro API
     │
     ▼
  poller.py  ──────────────────────────────────► arrivals.csv
     │  (stb-arrivals topic)
     ▼
  Kafka
     │
     ├──► crossing_detector.py ──────────────► crossings.csv
     │         │  (bus-crossings topic)
     │         ▼
     │       Kafka
     │         │
     ├─────────┴──► dashboard/app.py
     │
     └──► session_tracker.py ──────────────► sessions.json
                                          ► journeys.csv
```

---

## Tech stack

| Layer | Technology |
|---|---|
| API source | mo-bi.ro `nextArrivals` endpoint |
| Proxy | Cloudflare Worker (EC2 IPs blocked by mo-bi.ro) |
| Ingestion | Python · `requests` · `kafka-python` |
| Message bus | Apache Kafka (KRaft mode, no Zookeeper) |
| Persistence | CSV + JSON on EC2 host (host-mounted Docker volume) |
| Dashboard | Streamlit |
| Infrastructure | AWS EC2 t2.small · Docker Compose |

---

## Monitored stops

### Direction 0 — → Piața Română

| seq | Stop | Stop ID |
|---|---|---|
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

## Data files

Written to `/home/bus381/data/` on the EC2 host, mounted into containers at `/data/`.

**`arrivals.csv`** — one row per stop per poll:
```
ingested_at, stop_id, stop_name, direction, corridor_seq, arriving_in_seconds, is_timetable
```

**`crossings.csv`** — one row per detected bus crossing:
```
crossed_at, stop_id, stop_name, direction, corridor_seq, eta_before, eta_after
```

**`journeys.csv`** — one row per complete Gh. Sincai → Piața Română journey:
```
session_id,
sincai_at, sincai_eta_before, sincai_eta_after,
marasesti_at, marasesti_eta_before, marasesti_eta_after,
sf_gheorghe_at, ..., romana_at, ...,
total_seconds
```

**`sessions.json`** — in-progress session state snapshot (written after every crossing event).

---

## Project structure

```
bus381/
├── ingestion/
│   ├── config.py               # stop IDs, Kafka topics, poll intervals
│   ├── poller.py               # polls all stops, adaptive fast-poll at Gh. Sincai
│   ├── crossing_detector.py    # detects crossings via ETA reset, publishes bus-crossings
│   └── session_tracker.py      # sessionizes crossings into journeys, writes journeys.csv
├── dashboard/
│   └── app.py                  # Streamlit live board + corridor stats
├── docker-compose.yml          # Kafka + poller + detector + session-tracker + dashboard
└── Dockerfile
```

---

## Deployment

Runs on EC2 via Docker Compose. GitHub Actions deploys on every push to `main`.

```bash
# manual deploy on EC2
docker compose up -d --build
```

Data directory created automatically on first run:
```
/home/bus381/data/
├── arrivals.csv
├── crossings.csv
├── journeys.csv
└── sessions.json
```

---

## Roadmap

### Done ✅
- Poll mo-bi.ro every 45s, both directions, adaptive fast-poll at Gh. Sincai
- Crossing detection via ETA reset pattern (±45s accuracy)
- FIFO sessionization with 15-min plausibility guard
- Corridor stats: avg / best / worst in dashboard
- Journey matrix: last 10 completed buses with per-stop elapsed times

### Next
- Dir1 sessionization (Piața Română → Gh. Sincai)
- ETA accuracy analysis: compare `eta_before` to actual segment times
- Per-stop historical benchmarks in dashboard (avg segment time alongside live reading)

---

*Data sourced from [mo-bi.ro](https://mo-bi.ro) — Mobilitate în București.*
