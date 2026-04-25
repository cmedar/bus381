import time
import requests
import streamlit as st
from datetime import datetime, timezone

st.set_page_config(
    page_title="Bus 381 · Gh. Sincai",
    page_icon="🚌",
    layout="centered",
)

PROXY          = "https://crimson-river-eb3a.ciprian-medar.workers.dev"
ROUTE_ID       = "184"
STOP_DIR0      = 3782   # Gh. Sincai → Clabucet / Piata Romana
STOP_DIR1      = 3784   # Gh. Sincai → Piata Resita
REFRESH_S      = 60
HISTORY_SIZE   = 2

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, */*",
    "Referer": "https://maps.mo-bi.ro/",
    "Origin": "https://maps.mo-bi.ro",
}

for key in ("hist_dir0", "hist_dir1"):
    if key not in st.session_state:
        st.session_state[key] = []


def fetch_next_381(stop_id: int) -> dict | None:
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


def fmt(arriving_s: int) -> str:
    m, s = divmod(arriving_s, 60)
    return f"{m}m {s:02d}s"


def push(history_key: str, line: dict):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    st.session_state[history_key].append({
        "time":     now,
        "arriving": fmt(int(line["arrivingTime"])),
        "live":     not line.get("isTimetable", True),
    })
    st.session_state[history_key] = st.session_state[history_key][-HISTORY_SIZE:]


def render_stop(title: str, history_key: str):
    st.subheader(title)
    rows = st.session_state[history_key]
    if not rows:
        st.info("Waiting for first reading...")
        return
    for row in reversed(rows):
        tag = "live" if row["live"] else "timetable"
        st.metric(label=f"{row['time']} ({tag})", value=row["arriving"])


# ── fetch ──────────────────────────────────────────────────────────────────
dir0 = fetch_next_381(STOP_DIR0)
dir1 = fetch_next_381(STOP_DIR1)

if dir0:
    push("hist_dir0", dir0)
if dir1:
    push("hist_dir1", dir1)

# ── render ─────────────────────────────────────────────────────────────────
st.title("🚌 Bus 381 · Colegiul Gh. Sincai")
st.caption("Next bus at Gh. Sincai · both directions · 60s refresh")

col0, col1 = st.columns(2)
with col0:
    render_stop("→ Clabucet / Piata Romana", "hist_dir0")
with col1:
    render_stop("→ Piata Resita", "hist_dir1")

# ── auto-refresh ───────────────────────────────────────────────────────────
time.sleep(REFRESH_S)
st.rerun()
