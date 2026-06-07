import os
import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from crypto_arbitrage_aws.database import DatabaseSettings, connect_postgres
from crypto_arbitrage_aws.paths import PROJECT_ROOT

# ---------------------------------------------------------------------------
# Config — same env vars as processor.py
# DB_PATH anchored to the project directory so Streamlit finds the right file
# regardless of where it is launched from.
# ---------------------------------------------------------------------------
DB_SETTINGS             = DatabaseSettings.from_env()
DB_TYPE                 = DB_SETTINGS.db_type
DB_PATH                 = os.environ.get("DB_PATH", str(PROJECT_ROOT / "arbitrage.db"))
REFRESH_INTERVAL        = int(os.environ.get("REFRESH_INTERVAL", "30"))
ARBITRAGE_THRESHOLD_PCT = float(os.environ.get("ARBITRAGE_THRESHOLD_PCT", "0.5"))

st.set_page_config(
    page_title="Crypto Arbitrage Monitor",
    page_icon="📊",
    layout="wide",
)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_connection():
    if DB_TYPE == "postgres":
        return connect_postgres(DB_SETTINGS)
    return sqlite3.connect(DB_PATH)


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_opportunities(hours: int) -> pd.DataFrame:
    """Loads opportunities from the last N hours."""
    try:
        conn = get_connection()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        ph = "%s" if DB_TYPE == "postgres" else "?"
        df = pd.read_sql_query(
            f"""
            SELECT detected_at, coin, exchange_low, exchange_high,
                   price_low, price_high, spread_pct, source_mode
            FROM arbitrage_opportunities
            WHERE detected_at >= {ph}
            ORDER BY detected_at DESC
            """,
            conn,
            params=(cutoff,),
        )
        conn.close()
        df["detected_at"] = pd.to_datetime(df["detected_at"], utc=True)
        return df
    except Exception as e:
        st.warning(f"DB not reachable: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

st_autorefresh(interval=REFRESH_INTERVAL * 1000, key="autorefresh")

st.title("Crypto Arbitrage Monitor")
st.caption(
    f"Auto-refresh every {REFRESH_INTERVAL}s · "
    f"DB: {DB_TYPE.upper()} · "
    f"Last update: {datetime.now().strftime('%H:%M:%S')}"
)

# --- Sidebar ---
with st.sidebar:
    st.header("Filters")
    hours     = st.slider("Time window (hours)", 1, 72, 24)
    threshold = st.slider("Highlight spreads above (%)", 0.1, 5.0, ARBITRAGE_THRESHOLD_PCT, step=0.1)
    if st.button("Force refresh"):
        st.cache_data.clear()
        st.rerun()

df = load_opportunities(hours)

# ---------------------------------------------------------------------------
# Empty state
# ---------------------------------------------------------------------------
if df.empty:
    st.info(
        "No data yet. Run `crypto-arbitrage-processor` locally to start collecting "
        "opportunities, or wait for the Lambda Processor to emit results."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Metrics row
# ---------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

col1.metric("Opportunities", len(df))
col2.metric("Best spread", f"{df['spread_pct'].max():.4f}%")
col3.metric(
    "Most active coin",
    df["coin"].value_counts().idxmax(),
    f"{df['coin'].value_counts().iloc[0]} detections",
)
col4.metric(
    "Last detected",
    df["detected_at"].iloc[0].strftime("%H:%M:%S"),
)

st.divider()

# ---------------------------------------------------------------------------
# Opportunities table
# ---------------------------------------------------------------------------
st.subheader("Detected opportunities")

display = df.copy()
display["detected_at"] = display["detected_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
display["route"]       = display["exchange_low"] + " → " + display["exchange_high"]
display["price_low"]   = display["price_low"].map("${:,.4f}".format)
display["price_high"]  = display["price_high"].map("${:,.4f}".format)

display = display[[
    "detected_at", "coin", "route",
    "price_low", "price_high", "spread_pct", "source_mode",
]].rename(columns={
    "detected_at":  "Time (UTC)",
    "coin":         "Coin",
    "route":        "Buy → Sell",
    "price_low":    "Buy price",
    "price_high":   "Sell price",
    "spread_pct":   "Spread %",
    "source_mode":  "Mode",
})

st.dataframe(
    display.style.apply(
        lambda row: [
            "background-color: #d4edda" if row["Spread %"] >= threshold else ""
            for _ in row
        ],
        axis=1,
    ),
    use_container_width=True,
    hide_index=True,
)

st.divider()

# ---------------------------------------------------------------------------
# Spread over time chart
# ---------------------------------------------------------------------------
st.subheader("Spread % over time (top 5 coins)")

top_coins = df["coin"].value_counts().head(5).index.tolist()
chart_df  = df[df["coin"].isin(top_coins)].copy()
pivot     = chart_df.pivot_table(
    index="detected_at", columns="coin", values="spread_pct", aggfunc="mean"
)

if not pivot.empty:
    st.line_chart(pivot)

st.divider()

# ---------------------------------------------------------------------------
# Exchange pair breakdown
# ---------------------------------------------------------------------------
st.subheader("Detections by exchange pair")

pairs = (
    df.groupby(["exchange_low", "exchange_high"])
    .size()
    .reset_index(name="Detections")
)
pairs["Pair"] = pairs["exchange_low"] + " → " + pairs["exchange_high"]

st.bar_chart(pairs.set_index("Pair")["Detections"])
