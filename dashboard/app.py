"""Port congestion dashboard.

Reads the Delta tables directly (no Spark needed) and refreshes every minute:
gold/port_congestion for the counts and trends, and the last few minutes of
silver/ship_positions for the live map.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pydeck as pdk
import streamlit as st
from deltalake import DeltaTable

LAKE = os.getenv("LAKE_PATH", "../data/lake")
PORTS_FILE = os.getenv("PORTS_FILE", "../config/ports.json")
# how far back the map and the "ships tracked" number look
RECENT_MINUTES = int(os.getenv("RECENT_MINUTES", "30"))
EARTH_RADIUS_KM = 6371.0

SILVER = f"{LAKE}/silver/ship_positions"
GOLD = f"{LAKE}/gold/port_congestion"

ACTIVITIES = ["waiting", "berthed", "moving"]
ACTIVITY_COLORS = {
    "waiting": "#d1495b",
    "berthed": "#2e86de",
    "moving": "#27ae60",
    "unknown": "#95a5a6",
}

st.set_page_config(page_title="Port congestion", page_icon="⚓", layout="wide")


def table_exists(path):
    return os.path.isdir(os.path.join(path, "_delta_log"))


def hex_to_rgb(color):
    return [int(color[i:i + 2], 16) for i in (1, 3, 5)]


@st.cache_data
def load_ports():
    with open(PORTS_FILE) as f:
        return pd.DataFrame(json.load(f))


@st.cache_data(ttl=55)
def load_gold():
    if not table_exists(GOLD):
        return None
    df = DeltaTable(GOLD).to_pandas()
    return df.sort_values("window_start") if not df.empty else None


@st.cache_data(ttl=55)
def load_recent_positions(minutes):
    if not table_exists(SILVER):
        return None
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    # the date filter skips whole day folders; the time filter uses Parquet stats
    filter_ = (ds.field("event_date") >= since.date()) & (
        ds.field("event_time") >= pa.scalar(since, type=pa.timestamp("us", tz="UTC"))
    )
    table = DeltaTable(SILVER).to_pyarrow_dataset().to_table(
        columns=["mmsi", "ship_name", "lat", "lon", "sog_knots", "activity", "event_time"],
        filter=filter_,
    )
    return table.to_pandas()


def latest_per_ship(positions):
    return positions.sort_values("event_time").drop_duplicates("mmsi", keep="last")


def ships_near_ports(ships, ports):
    """Ships inside a port circle, tagged with that port (same geofence as the gold job)."""
    lat1, lon1 = np.radians(ships["lat"].to_numpy())[:, None], np.radians(ships["lon"].to_numpy())[:, None]
    lat2, lon2 = np.radians(ports["lat"].to_numpy())[None, :], np.radians(ports["lon"].to_numpy())[None, :]
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    distance_km = 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))  # one row per ship, one column per port
    inside = distance_km <= ports["radius_km"].to_numpy()[None, :]
    has_port = inside.any(axis=1)
    near = ships[has_port].copy()
    near["port"] = ports["name"].to_numpy()[inside[has_port].argmax(axis=1)]
    return near


def show_kpis(gold, positions, ports):
    latest = gold[gold["window_start"] == gold["window_start"].max()] if gold is not None else None
    tracked_since = datetime.now(timezone.utc) - timedelta(minutes=15)
    tracked = positions.loc[positions["event_time"] >= tracked_since, "mmsi"].nunique() if positions is not None else 0

    cols = st.columns(4)
    cols[0].metric("Ships tracked (last 15 min)", f"{tracked:,}")
    cols[1].metric("Ports monitored", len(ports))
    cols[2].metric("Ships waiting (latest window)", int(latest["ships_waiting"].sum()) if latest is not None else "–")
    cols[3].metric(
        "Latest window (UTC)",
        f"{latest['window_start'].iloc[0]:%H:%M}–{latest['window_end'].iloc[0]:%H:%M}" if latest is not None else "–",
    )


def show_leaderboard(gold):
    st.subheader("Congestion leaderboard")
    latest = gold[gold["window_start"] == gold["window_start"].max()].sort_values("ships_waiting", ascending=False)
    st.caption(f"Ships per port at the end of the window starting {latest['window_start'].iloc[0]:%Y-%m-%d %H:%M} UTC")
    st.bar_chart(
        latest.rename(columns={f"ships_{a}": a for a in ACTIVITIES}),
        x="port",
        y=ACTIVITIES,
        color=[ACTIVITY_COLORS[a] for a in ACTIVITIES],
        horizontal=True,
        height=380,
    )
    with st.expander("Table"):
        st.dataframe(
            latest[["port", "country", "ships_in_area", "ships_waiting", "ships_berthed", "ships_moving",
                     "avg_sog_knots", "position_reports"]],
            hide_index=True,
            use_container_width=True,
        )


def show_trend(gold):
    st.subheader("Ships waiting over time")
    latest = gold[gold["window_start"] == gold["window_start"].max()]
    top_ports = latest.sort_values("ships_waiting", ascending=False)["port"].head(3).tolist()
    selected = st.multiselect("Ports", sorted(gold["port"].unique()), default=top_ports)
    if not selected:
        st.info("Pick at least one port.")
        return
    trend = (
        gold[gold["port"].isin(selected)]
        .pivot_table(index="window_start", columns="port", values="ships_waiting", aggfunc="sum")
    )
    st.line_chart(trend, height=380)
    st.caption("The first window after a start is partial, so its counts are low.")


def show_map(positions, ports):
    st.subheader(f"Ships near ports (last {RECENT_MINUTES} min)")
    choice = st.selectbox("Zoom to", ["All ports"] + ports["name"].tolist())

    ships = ships_near_ports(latest_per_ship(positions), ports) if positions is not None and not positions.empty else None
    if ships is None or ships.empty:
        st.info("No ships inside the port areas yet.")
        return

    ships["color"] = ships["activity"].map(lambda a: hex_to_rgb(ACTIVITY_COLORS.get(a, ACTIVITY_COLORS["unknown"])))
    ships["sog_label"] = ships["sog_knots"].map(lambda s: "unknown" if pd.isna(s) else f"{s:.1f} kn")
    ships["ship_name"] = ships["ship_name"].fillna("").replace("", "(no name)")
    ships["seen"] = ships["event_time"].dt.strftime("%H:%M:%S UTC")

    if choice == "All ports":
        view = pdk.ViewState(latitude=20, longitude=40, zoom=1.2)
    else:
        port = ports[ports["name"] == choice].iloc[0]
        view = pdk.ViewState(latitude=port["lat"], longitude=port["lon"], zoom=9)

    port_layer = pdk.Layer(
        "ScatterplotLayer",
        data=ports.assign(radius_m=ports["radius_km"] * 1000),
        get_position="[lon, lat]",
        get_radius="radius_m",
        get_fill_color=[255, 165, 0, 25],
        get_line_color=[255, 140, 0, 200],
        stroked=True,
        line_width_min_pixels=1,
    )
    ship_layer = pdk.Layer(
        "ScatterplotLayer",
        data=ships[["lon", "lat", "color", "ship_name", "mmsi", "activity", "sog_label", "port", "seen"]],
        get_position="[lon, lat]",
        get_fill_color="color",
        get_radius=250,
        radius_min_pixels=3,
        pickable=True,
    )
    st.pydeck_chart(
        pdk.Deck(
            layers=[port_layer, ship_layer],
            initial_view_state=view,
            map_style=pdk.map_styles.CARTO_LIGHT,
            tooltip={"text": "{ship_name} (MMSI {mmsi})\n{activity}, {sog_label}\n{port}, seen {seen}"},
        ),
        height=520,
    )
    legend = " ".join(
        f"<span style='color:{ACTIVITY_COLORS[a]}'>●</span> {a}" for a in ACTIVITIES + ["unknown"]
    )
    st.markdown(legend + " &nbsp; <span style='color:#ff8c00'>◯</span> port area", unsafe_allow_html=True)


@st.fragment(run_every="60s")
def dashboard():
    ports = load_ports()
    gold = load_gold()
    positions = load_recent_positions(RECENT_MINUTES)

    if positions is None:
        st.info("No data yet: waiting for the silver table. Check that the producer and spark-positions are running.")
        return

    show_kpis(gold, positions, ports)
    st.divider()

    if gold is None:
        st.info(
            "The first congestion window hasn't finished yet. It's written once a window closes and the "
            "late-data allowance passes, up to about 25 minutes after the first start."
        )
    else:
        left, right = st.columns(2)
        with left:
            show_leaderboard(gold)
        with right:
            show_trend(gold)
    st.divider()

    show_map(positions, ports)
    st.caption(f"Refreshes every minute · last refresh {datetime.now(timezone.utc):%H:%M:%S} UTC")


st.title("⚓ Port congestion")
st.caption("Live AIS ship positions → Kafka → Spark Structured Streaming → Delta Lake")
dashboard()
