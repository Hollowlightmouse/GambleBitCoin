"""
Streamlit Dashboard for GambleBitCoin Streaming Pipeline
=========================================================
Reads Flink-processed window aggregation data from Redis
and displays real-time metrics in a web dashboard.

Variables from .env:
  REDIS_URL - Redis connection string
"""

import os
import json
import time
from datetime import datetime

import streamlit as st
import redis
import pandas as pd
import plotly.graph_objects as go

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
REDIS_HOST = REDIS_URL.split("://")[-1].split(":")[0] if "://" in REDIS_URL else "redis"
REDIS_PORT = int(REDIS_URL.split("://")[-1].split(":")[1].split("/")[0]) if "://" in REDIS_URL else 6379


st.set_page_config(
    page_title="GambleBitCoin - Streaming Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("📊 GambleBitCoin Streaming Pipeline Dashboard")
st.caption("Apache Flink Time Window Aggregations")


@st.cache_resource
def get_redis_client():
    """Create and cache Redis connection."""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, socket_connect_timeout=5)


def get_window_keys(r):
    """Get all Flink window keys from Redis."""
    try:
        keys = r.smembers("flink:window:keys")
        return [k.decode("utf-8") if isinstance(k, bytes) else k for k in keys]
    except Exception:
        return []


def get_window_data(r, key):
    """Get window aggregation data from Redis."""
    try:
        data = r.hgetall(key)
        if not data:
            return None
        result = {}
        for k, v in data.items():
            key_str = k.decode("utf-8") if isinstance(k, bytes) else k
            val_str = v.decode("utf-8") if isinstance(v, bytes) else v
            result[key_str] = val_str
        return result
    except Exception:
        return None


def load_metrics():
    """Load all Flink window metrics from Redis."""
    r = get_redis_client()
    keys = get_window_keys(r)

    records = []
    for key in keys:
        data = get_window_data(r, key)
        if data:
            try:
                records.append({
                    "symbol": data.get("symbol", ""),
                    "avg_price": float(data.get("avg_price", 0)),
                    "max_price": float(data.get("max_price", 0)),
                    "min_price": float(data.get("min_price", 0)),
                    "trade_count": int(data.get("trade_count", 0)),
                    "total_volume": float(data.get("total_volume", 0)),
                    "window_ts": int(data.get("window_ts", 0)),
                })
            except (ValueError, TypeError):
                continue

    return records


# ========================
# DASHBOARD LAYOUT
# ========================

auto_refresh = st.sidebar.checkbox("Auto-refresh (5s)", value=True)
if auto_refresh:
    st_autorefresh = st.empty()
    time.sleep(0)
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.info(f"**Redis**: `{REDIS_HOST}:{REDIS_PORT}`")

# Load data
records = load_metrics()

if not records:
    st.warning("No streaming data available yet. Ensure Kafka producer and Flink job are running.")
    st.info("Data will appear here once Flink processes trades from Kafka.")
    st.stop()

df = pd.DataFrame(records)

# ========================
# KPI CARDS
# ========================

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_trades = int(df["trade_count"].sum())
    st.metric("Total Trades (all windows)", f"{total_trades:,}")

with col2:
    total_volume = df["total_volume"].sum()
    st.metric("Total Volume", f"{total_volume:,.4f}")

with col3:
    avg_price = df["avg_price"].mean()
    st.metric("Avg Price (all symbols)", f"{avg_price:,.2f}")

with col4:
    window_count = len(df)
    st.metric("Active Windows", window_count)

# ========================
# PRICE CHART BY SYMBOL
# ========================

st.subheader("Average Price by Symbol")

symbols = df["symbol"].unique()
fig = go.Figure()

for symbol in sorted(symbols):
    symbol_data = df[df["symbol"] == symbol].sort_values("window_ts")
    if not symbol_data.empty:
        fig.add_trace(go.Scatter(
            x=symbol_data["window_ts"].apply(lambda t: datetime.fromtimestamp(t / 1000)),
            y=symbol_data["avg_price"],
            mode="lines+markers",
            name=symbol,
            hovertemplate="<b>%{fullData.name}</b><br>Time: %{x}<br>Avg Price: $%{y:.4f}<extra></extra>",
        ))

fig.update_layout(
    xaxis_title="Time",
    yaxis_title="Price (USDT)",
    hovermode="x unified",
    template="plotly_dark",
    height=400,
)

st.plotly_chart(fig, use_container_width=True)

# ========================
# MIN/MAX RANGE CHART
# ========================

st.subheader("Price Range (Min - Max) by Window")

fig_range = go.Figure()

for symbol in sorted(symbols):
    symbol_data = df[df["symbol"] == symbol].sort_values("window_ts")
    if not symbol_data.empty:
        fig_range.add_trace(go.Scatter(
            x=symbol_data["window_ts"].apply(lambda t: datetime.fromtimestamp(t / 1000)),
            y=symbol_data["max_price"],
            mode="lines",
            name=f"{symbol} Max",
            line=dict(color="green", dash="dash"),
            hovertemplate="<b>%{fullData.name}</b><br>Time: %{x}<br>Max: $%{y:.4f}<extra></extra>",
        ))
        fig_range.add_trace(go.Scatter(
            x=symbol_data["window_ts"].apply(lambda t: datetime.fromtimestamp(t / 1000)),
            y=symbol_data["min_price"],
            mode="lines",
            name=f"{symbol} Min",
            line=dict(color="red", dash="dash"),
            hovertemplate="<b>%{fullData.name}</b><br>Time: %{x}<br>Min: $%{y:.4f}<extra></extra>",
            fill="tonexty",
            fillcolor="rgba(128,128,128,0.1)",
        ))

fig_range.update_layout(
    xaxis_title="Time",
    yaxis_title="Price (USDT)",
    hovermode="x unified",
    template="plotly_dark",
    height=400,
)

st.plotly_chart(fig_range, use_container_width=True)

# ========================
# TRADE COUNT & VOLUME
# ========================

col_trade, col_vol = st.columns(2)

with col_trade:
    st.subheader("Trade Count per Window")
    fig_count = go.Figure()
    for symbol in sorted(symbols):
        symbol_data = df[df["symbol"] == symbol].sort_values("window_ts")
        if not symbol_data.empty:
            fig_count.add_trace(go.Bar(
                x=symbol_data["window_ts"].apply(lambda t: datetime.fromtimestamp(t / 1000)),
                y=symbol_data["trade_count"],
                name=symbol,
                hovertemplate="<b>%{fullData.name}</b><br>Time: %{x}<br>Trades: %{y}<extra></extra>",
            ))
    fig_count.update_layout(
        xaxis_title="Time",
        yaxis_title="Number of Trades",
        barmode="group",
        template="plotly_dark",
        height=350,
    )
    st.plotly_chart(fig_count, use_container_width=True)

with col_vol:
    st.subheader("Total Volume per Window")
    fig_vol = go.Figure()
    for symbol in sorted(symbols):
        symbol_data = df[df["symbol"] == symbol].sort_values("window_ts")
        if not symbol_data.empty:
            fig_vol.add_trace(go.Bar(
                x=symbol_data["window_ts"].apply(lambda t: datetime.fromtimestamp(t / 1000)),
                y=symbol_data["total_volume"],
                name=symbol,
                hovertemplate="<b>%{fullData.name}</b><br>Time: %{x}<br>Volume: %{y:.4f}<extra></extra>",
            ))
    fig_vol.update_layout(
        xaxis_title="Time",
        yaxis_title="Volume",
        barmode="group",
        template="plotly_dark",
        height=350,
    )
    st.plotly_chart(fig_vol, use_container_width=True)

# ========================
# RAW DATA TABLE
# ========================

st.subheader("Raw Window Data")

display_df = df.copy()
display_df["window_time"] = pd.to_datetime(display_df["window_ts"], unit="ms")
display_df = display_df[["window_time", "symbol", "avg_price", "max_price", "min_price", "trade_count", "total_volume"]]
display_df = display_df.sort_values("window_time", ascending=False)

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "window_time": st.column_config.DatetimeColumn("Window Time", format="YYYY-MM-DD HH:mm:ss"),
        "symbol": st.column_config.TextColumn("Symbol"),
        "avg_price": st.column_config.NumberColumn("Avg Price", format="$%.4f"),
        "max_price": st.column_config.NumberColumn("Max Price", format="$%.4f"),
        "min_price": st.column_config.NumberColumn("Min Price", format="$%.4f"),
        "trade_count": st.column_config.NumberColumn("Trades", format="%d"),
        "total_volume": st.column_config.NumberColumn("Volume", format="%.4f"),
    },
)
