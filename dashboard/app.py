import time
import requests
import streamlit as st
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(
    page_title="Bus 381 · Live Arrivals",
    page_icon="🚌",
    layout="wide",
)

PROXY      = "https://crimson-river-eb3a.ciprian-medar.workers.dev"
ROUTE_ID   = "184"
REFRESH_S  = 40

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


def fetch_all(stops: list[tuple]) -> dict[int, dict | None]:
    results = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fetch_eta, stop_id): stop_id for stop_id, _ in stops}
        for f in as_completed(futures):
            results[futures[f]] = f.result()
    return results


def fmt(arriving_s: int) -> str:
    m, s = divmod(arriving_s, 60)
    return f"{m}m {s:02d}s"


def render_board(stops: list[tuple], results: dict):
    for stop_id, name in stops:
        line = results.get(stop_id)
        col_name, col_eta, col_src = st.columns([3, 1, 1])
        col_name.write(f"**{name}**")
        if line:
            arriving_s = int(line.get("arrivingTime", 0))
            is_live    = not line.get("isTimetable", True)
            col_eta.write(f"**{fmt(arriving_s)}**")
            col_src.write("🟢 live" if is_live else "🕐 schedule")
        else:
            col_eta.write("—")
            col_src.write("")


# ── fetch all stops in parallel ────────────────────────────────────────────
now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
results_dir0 = fetch_all(STOPS_DIR0)
results_dir1 = fetch_all(STOPS_DIR1)

# ── render ─────────────────────────────────────────────────────────────────
st.title("🚌 Bus 381 · Live Arrivals")
st.caption(f"Last updated: {now} · refreshes every {REFRESH_S}s")

col0, col1 = st.columns(2)

with col0:
    st.subheader("→ Piata Romana / Clabucet")
    render_board(STOPS_DIR0, results_dir0)

with col1:
    st.subheader("→ Piata Resita")
    render_board(STOPS_DIR1, results_dir1)

# ── auto-refresh ───────────────────────────────────────────────────────────
time.sleep(REFRESH_S)
st.rerun()
