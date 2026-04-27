import os
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
[data-testid="stLayoutWrapper"] { max-width: 600px; }
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
SESSIONS_JSON   = os.path.join(DATA_DIR, "sessions.json")

# stop_id → corridor seq (dir0, Sincai=1 … Romana=7)
STOP_SEQ = {3782: 1, 3678: 2, 7257: 3, 7256: 4, 12353: 5, 12354: 6, 6588: 7}
BUS_LABELS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, */*",
    "Referer": "https://maps.mo-bi.ro/",
    "Origin": "https://maps.mo-bi.ro",
}

STOPS_DIR0 = [
    (3688,  "Visana"),
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


def load_sessions() -> list[dict]:
    try:
        with open(SESSIONS_JSON) as f:
            return json.load(f)
    except Exception:
        return []


def fmt_elapsed(sessions: list, seq: int) -> str:
    parts = []
    for i, s in enumerate(sessions):
        label = BUS_LABELS[i] if i < len(BUS_LABELS) else str(i + 1)
        crossings = s.get("crossings", {})
        if str(seq) not in crossings:
            parts.append(f"{label}:—")
        elif seq == 1:
            parts.append(f"{label}:●")
        else:
            start  = datetime.fromisoformat(crossings["1"])
            stop   = datetime.fromisoformat(crossings[str(seq)])
            delta  = int((stop - start).total_seconds() // 60)
            parts.append(f"{label}:{delta}m")
    # drop trailing dashes — buses that clearly haven't reached this stop
    while parts and parts[-1].endswith(":—"):
        parts.pop()
    return "  ".join(parts)


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


def render_board(stops: list[tuple], results: dict, crossings: dict, sessions: list = None):
    for stop_id, name in stops:
        line     = results.get(stop_id)
        last_bus = crossings.get(str(stop_id), "—")
        is_live  = line is not None and not line.get("isTimetable", True)
        dot      = "🟢" if is_live else "🔘"

        c0, c1 = st.columns([2, 3])
        c0.write(f"⛩️ {name}")
        if line:
            arriving_s = int(line.get("arrivingTime", 0))
            arrives_at = (datetime.now(BUCHAREST_TZ) + timedelta(seconds=arriving_s)).strftime("%H:%M")
            last_str   = f"🚍 {last_bus}" if last_bus != "—" else "—"
            eta_line   = f"{dot} {fmt(arriving_s)} · 🚌 {arrives_at} · {last_str}"
        else:
            eta_line = "—"

        seq = STOP_SEQ.get(stop_id)
        if sessions and seq:
            matrix = fmt_elapsed(sessions, seq)
            c1.write(f"{eta_line}\n\n`{matrix}`")
        else:
            c1.write(eta_line)


# ── fetch ──────────────────────────────────────────────────────────────────
now          = datetime.now(BUCHAREST_TZ).strftime("%H:%M:%S")
crossings    = load_last_crossings()
sessions     = load_sessions()
results_dir0 = fetch_all(tuple(sid for sid, _ in STOPS_DIR0))
results_dir1 = fetch_all(tuple(sid for sid, _ in STOPS_DIR1))

# ── render ─────────────────────────────────────────────────────────────────
st.title("🚌 Bus 381 · Live Arrivals")
st.markdown(f"<span style='font-size:2rem'>{now}</span>", unsafe_allow_html=True)

st.subheader("→ Piata Romana")
render_board(STOPS_DIR0, results_dir0, crossings, sessions)

st.subheader("→ Tineretului")
render_board(STOPS_DIR1, results_dir1, crossings)

# ── auto-refresh ───────────────────────────────────────────────────────────
time.sleep(REFRESH_S)
st.rerun()
