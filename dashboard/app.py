import os
import csv
import time
import json
import requests
import streamlit as st
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaConsumer

st.set_page_config(
    page_title="Bus 381 · Live Arrivals",
    page_icon="🚌",
    layout="centered",
)

st.markdown("""
<style>
[data-testid="stMarkdownContainer"] strong { font-weight: 800; font-size: 1rem; }
[data-testid="stLayoutWrapper"] { max-width: 800px; }
[data-testid="stMainMenu"] { display: none; }
[data-testid="stBaseButton-header"] { display: none; }
.st-emotion-cache-lvs4k2 { display: none; }
</style>
""", unsafe_allow_html=True)

PROXY           = "https://crimson-river-eb3a.ciprian-medar.workers.dev"
ROUTE_ID        = "184"
REFRESH_S       = 10
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CROSSINGS_TOPIC = "bus-crossings"
BUCHAREST_TZ    = timezone(timedelta(hours=3))
DATA_DIR        = os.getenv("DATA_DIR", "/data")
JOURNEYS_CSV      = os.path.join(DATA_DIR, "journeys.csv")
JOURNEYS_DIR1_CSV = os.path.join(DATA_DIR, "journeys_dir1.csv")

# stop_id → corridor seq
STOP_SEQ_DIR0 = {3782: 1, 3678: 2, 7257: 3, 7256: 4, 12353: 5, 12354: 6, 6588: 7}
STOP_SEQ_DIR1 = {3826: 1, 12514: 2, 7411: 3, 7462: 4, 6611: 5, 3667: 6, 3784: 7}
STOP_SEQ      = STOP_SEQ_DIR0  # kept for crossings lookup (dir0 only)
BUS_LABELS    = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, */*",
    "Referer": "https://maps.mo-bi.ro/",
    "Origin": "https://maps.mo-bi.ro",
}

STOPS_DIR0 = [
    (3782,  "Gh. Sincai"),
    (3678,  "Bd. Marasesti"),
    (7257,  "Piata Sf. Gheorghe"),
    (7256,  "Universitate"),
    (12353, "Bd. Nicolae Balcescu"),
    (12354, "Arthur Verona"),
    (6588,  "Piata Romana"),
]

STOPS_DIR1 = [
    (5972,  "Orlando"),
    (3826,  "Piata Romana"),
    (12514, "George Enescu"),
    (7411,  "Bd. Nicolae Balcescu"),
    (7462,  "Piata 21 Decembrie 1989"),
    (6611,  "Piata Sf. Gheorghe"),
    (3667,  "Bd. Marasesti"),
    (3784,  "Gh. Sincai"),
]


def load_journeys(path: str = JOURNEYS_CSV) -> list[dict]:
    try:
        with open(path, newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


SEQ_TO_CSV_COL_DIR0 = {
    1: "sincai_at", 2: "marasesti_at", 3: "sf_gheorghe_at",
    4: "universitate_at", 5: "nicolae_balcescu_at",
    6: "arthur_verona_at", 7: "romana_at",
}
SEQ_TO_CSV_COL_DIR1 = {
    1: "romana_at", 2: "enescu_at", 3: "balcescu_at",
    4: "piata21_at", 5: "sf_gheorghe_at", 6: "marasesti_at", 7: "sincai_at",
}
SEQ_TO_CSV_COL = SEQ_TO_CSV_COL_DIR0  # default, kept for compatibility


def _elapsed_from_journey(j: dict, seq: int, seq_to_col: dict, start_col: str) -> str:
    if seq == max(seq_to_col):
        total_s = j.get("total_seconds", "")
        return f"{int(total_s)//60}m" if total_s else "—"
    start = j.get(start_col, "")
    stop  = j.get(seq_to_col.get(seq, ""), "")
    if not start or not stop:
        return "—"
    if seq == 1:
        return "●"
    delta = int((datetime.fromisoformat(stop) - datetime.fromisoformat(start)).total_seconds() // 60)
    return f"{delta}m"


def fmt_elapsed(journeys: list, seq: int, seq_to_col: dict, start_col: str) -> str:
    window = (journeys or [])[-10:]
    parts  = []
    for i, j in enumerate(window):
        label = BUS_LABELS[i] if i < len(BUS_LABELS) else str(i + 1)
        parts.append(f"{label}:{_elapsed_from_journey(j, seq, seq_to_col, start_col)}")
    while parts and parts[-1].endswith(":—"):
        parts.pop()
    return "  ".join(parts)


def corridor_stats(journeys: list, n: int = 5) -> dict | None:
    today = datetime.now(BUCHAREST_TZ).date().isoformat()
    valid = [j for j in journeys
             if j.get("total_seconds") and 600 <= int(j["total_seconds"]) <= 2700]
    if not valid:
        return None
    times       = [int(j["total_seconds"]) for j in valid]
    today_times = [int(j["total_seconds"]) for j in valid
                   if j.get("sincai_at", "").startswith(today)]
    recent = times[-n:]
    return {
        "avg":         sum(recent) // len(recent),
        "best":        min(times),
        "worst":       max(times),
        "count":       len(times),
        "today_best":  min(today_times) if today_times else None,
        "today_worst": max(today_times) if today_times else None,
        "today_count": len(today_times),
    }


def render_stats(stats: dict | None):
    if not stats:
        st.caption("No corridor data yet.")
        return
    tb = f"{stats['today_best'] // 60}m"  if stats["today_best"]  else "—"
    tw = f"{stats['today_worst'] // 60}m" if stats["today_worst"] else "—"
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("⏱ Avg (last 5)",  f"{stats['avg'] // 60}m")
    c2.metric("📅 Today best",   tb)
    c3.metric("📅 Today worst",  tw)
    c4.metric("📅 Today rides",  stats["today_count"])


def render_stats_extended(stats: dict | None):
    if not stats:
        return
    e1, e2, e3 = st.columns(3)
    e1.metric("🏆 Best ever",  f"{stats['best'] // 60}m")
    e2.metric("🐌 Worst ever", f"{stats['worst'] // 60}m")
    e3.metric("📊 Journeys",   stats["count"])


def load_last_crossings() -> dict[str, str]:
    """Read bus-crossings topic, return {stop_id: local_time_str} for latest crossing per stop."""
    try:
        consumer = KafkaConsumer(
            CROSSINGS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda v: json.loads(v.decode()),
            auto_offset_reset="earliest",
            group_id=None,
            consumer_timeout_ms=2000,
        )
        last = {}
        for msg in consumer:
            rec       = msg.value
            stop_id   = rec["stop_id"]
            crossed   = datetime.fromisoformat(rec["crossed_at"]).astimezone(BUCHAREST_TZ)
            eta_before = rec.get("eta_before", 0)
            arrived   = crossed - timedelta(seconds=eta_before)
            last[stop_id] = arrived.strftime("%H:%M")
        consumer.close()
        return last
    except Exception:
        return {}


def fetch_eta(stop_id: int) -> dict | None:
    try:
        resp = requests.get(
            f"{PROXY}/api/nextArrivals/{stop_id}",
            headers=_HEADERS, timeout=10,
        )
        resp.raise_for_status()
        for line in resp.json().get("lines", []):
            if line.get("id") == ROUTE_ID:
                return line
    except Exception:
        pass
    return None


def fetch_batch(stops: list[tuple]) -> dict[int, dict | None]:
    results = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fetch_eta, stop_id): stop_id for stop_id, _ in stops}
        for f in as_completed(futures):
            results[futures[f]] = f.result()
    return results


@st.cache_data(ttl=45)
def fetch_all(stop_ids: tuple) -> dict[int, dict | None]:
    stops = [(sid, "") for sid in stop_ids]
    mid   = len(stops) // 2
    results = fetch_batch(stops[:mid])
    time.sleep(1)
    results.update(fetch_batch(stops[mid:]))
    return results


def fmt(arriving_s: int) -> str:
    m, s = divmod(arriving_s, 60)
    return f"{m}m {s:02d}s"


def render_board(stops: list[tuple], results: dict, crossings: dict):
    for stop_id, name in stops:
        line     = results.get(stop_id)
        last_bus = crossings.get(str(stop_id), "—")
        is_live  = line is not None and not line.get("isTimetable", True)
        dot      = "🟢" if is_live else "🔘"

        c0, c1 = st.columns([1, 3])
        c0.write(f"⛩️ {name}")
        if line:
            arriving_s = int(line.get("arrivingTime", 0))
            arrives_at = (datetime.now(BUCHAREST_TZ) + timedelta(seconds=arriving_s)).strftime("%H:%M")
            last_str   = f"🚍 {last_bus}" if last_bus != "—" else "—"
            c1.write(f"{dot} {fmt(arriving_s)} · 🚌 {arrives_at} · {last_str}")
        else:
            c1.write("—")


def render_matrix(stops: list[tuple], journeys: list, stop_seq: dict,
                  seq_to_col: dict, start_col: str):
    for stop_id, name in stops:
        seq    = stop_seq.get(stop_id)
        matrix = fmt_elapsed(journeys, seq, seq_to_col, start_col) if seq else ""
        if matrix:
            c0, c1 = st.columns([1, 3])
            c0.caption(f"⛩️ {name}")
            c1.caption(f"`{matrix}`")


# ── fetch ──────────────────────────────────────────────────────────────────
now           = datetime.now(BUCHAREST_TZ).strftime("%H:%M:%S")
crossings     = load_last_crossings()
journeys      = load_journeys()
journeys_dir1 = load_journeys(JOURNEYS_DIR1_CSV)
results_dir0  = fetch_all(tuple(sid for sid, _ in STOPS_DIR0))
results_dir1  = fetch_all(tuple(sid for sid, _ in STOPS_DIR1))

# ── render ─────────────────────────────────────────────────────────────────
stats0 = corridor_stats(journeys)
stats1 = corridor_stats(journeys_dir1)
avg0   = f"  ·  ~{stats0['avg'] // 60}m" if stats0 else ""
avg1   = f"  ·  ~{stats1['avg'] // 60}m" if stats1 else "  ·  no data yet"

st.title("🚌 Bus 381 · Live Arrivals")
st.markdown(f"<span style='font-size:2rem'>{now}</span>", unsafe_allow_html=True)

st.subheader(f"→ Piata Romana{avg0}")
render_stats(stats0)
render_board(STOPS_DIR0, results_dir0, crossings)
with st.expander("Journey matrix (last 10 buses)"):
    render_stats_extended(stats0)
    render_matrix(STOPS_DIR0, journeys, STOP_SEQ_DIR0, SEQ_TO_CSV_COL_DIR0, "sincai_at")

st.subheader(f"→ Tineretului{avg1}")
render_stats(stats1)
render_board(STOPS_DIR1, results_dir1, crossings)
with st.expander("Journey matrix (last 10 buses)"):
    render_stats_extended(stats1)
    render_matrix(STOPS_DIR1, journeys_dir1, STOP_SEQ_DIR1, SEQ_TO_CSV_COL_DIR1, "romana_at")

# ── auto-refresh ───────────────────────────────────────────────────────────
time.sleep(REFRESH_S)
st.rerun()
