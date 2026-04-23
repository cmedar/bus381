import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from databricks import sql

st.set_page_config(
    page_title="Bus 381 · Tineretului–Română",
    page_icon="🚌",
    layout="wide",
)

DATABRICKS_HOST      = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN     = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "")
GOLD_TABLE           = os.getenv("GOLD_TABLE", "bus381.gold.eta_baseline")
SILVER_TABLE         = os.getenv("SILVER_TABLE", "bus381.silver.sessions")

REFRESH_SECONDS = 30


@st.cache_data(ttl=REFRESH_SECONDS)
def load_gold() -> pd.DataFrame:
    with sql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {GOLD_TABLE} ORDER BY updated_at DESC LIMIT 1")
            return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


@st.cache_data(ttl=REFRESH_SECONDS)
def load_delay_heatmap() -> pd.DataFrame:
    with sql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT hour_of_day, day_of_week,
                       AVG(deviation_seconds) AS median_deviation
                FROM {SILVER_TABLE}
                WHERE is_complete = true
                GROUP BY hour_of_day, day_of_week
            """)
            return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


@st.cache_data(ttl=REFRESH_SECONDS)
def load_baseline_trend() -> pd.DataFrame:
    with sql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT run_start_ts, duration_seconds, deviation_seconds
                FROM {SILVER_TABLE}
                WHERE is_complete = true
                ORDER BY run_start_ts DESC
                LIMIT 200
            """)
            return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


def render_live_eta(gold: pd.DataFrame):
    st.subheader("Live ETA — next 381 at Piața Română")
    if gold.empty:
        st.info("Waiting for data — pipeline not yet running.")
        return

    row = gold.iloc[0]
    eta = row.get("eta_next_arrival", "—")
    lo  = int(row.get("confidence_low_s",  0)) // 60
    hi  = int(row.get("confidence_high_s", 0)) // 60
    dev = int(row.get("baseline_duration_s", 0)) // 60

    col1, col2, col3 = st.columns(3)
    col1.metric("ETA", str(eta)[:16] if eta != "—" else "—")
    col2.metric("Baseline duration", f"{dev} min")
    col3.metric("Confidence window", f"{lo}–{hi} min")


def render_delay_heatmap(df: pd.DataFrame):
    st.subheader("Delay heatmap — hour × day")
    if df.empty:
        st.info("No session data yet.")
        return

    days = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
    df["day_label"] = df["day_of_week"].map(days)
    pivot = df.pivot(index="day_label", columns="hour_of_day", values="median_deviation")

    fig = px.imshow(
        pivot,
        labels={"x": "Hour of day", "y": "Day", "color": "Avg delay (s)"},
        color_continuous_scale="RdYlGn_r",
        aspect="auto",
    )
    st.plotly_chart(fig, use_container_width=True)


def render_baseline_trend(df: pd.DataFrame):
    st.subheader("Baseline trend — session durations vs history")
    if df.empty:
        st.info("No completed sessions yet.")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["run_start_ts"], y=df["duration_seconds"],
        mode="lines+markers", name="Session duration (s)",
    ))
    st.plotly_chart(fig, use_container_width=True)


def main():
    st.title("🚌 Bus 381 · Tineretului–Română Transit")
    st.caption("Live ETA · Delay patterns · Self-building baseline · 30s refresh")

    try:
        gold   = load_gold()
        heatmap = load_delay_heatmap()
        trend   = load_baseline_trend()
    except Exception as e:
        st.error(f"Could not connect to Databricks: {e}")
        st.info("Dashboard will display data once the streaming pipeline is running.")
        gold = heatmap = trend = pd.DataFrame()

    render_live_eta(gold)
    st.divider()
    render_delay_heatmap(heatmap)
    st.divider()
    render_baseline_trend(trend)


if __name__ == "__main__":
    main()
