"""
Nordic Power Terminal — Streamlit dashboard.

Launch with:
    streamlit run dashboard.py
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from npt.data.storage.duckdb_store import DuckDBStore
from npt.settings import Settings

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Nordic Power Terminal",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Inject Georgia serif font and tighten a few spacing details
st.markdown(
    """
    <style>
    html, body, [class*="st-"], .stMarkdown, .stMetric,
    .stDataFrame, .stCaption, .stAlert, button, label,
    input, select, textarea {
        font-family: Georgia, "Times New Roman", Times, serif !important;
    }
    h1, h2, h3, h4, h5, h6 {
        font-family: Georgia, "Times New Roman", Times, serif !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

ZONE_COLOURS = {
    "NO1": "#4C72B0",
    "NO2": "#DD8452",
    "NO3": "#55A868",
    "NO4": "#C44E52",
    "NO5": "#8172B2",
}

ZONE_NAMES = {
    "NO1": "NO1 · Oslo",
    "NO2": "NO2 · Kristiansand",
    "NO3": "NO3 · Molde",
    "NO4": "NO4 · Tromsø",
    "NO5": "NO5 · Bergen",
}


# ---------------------------------------------------------------------------
# Data helpers (cached)
# ---------------------------------------------------------------------------


@st.cache_resource
def get_store() -> DuckDBStore:
    return DuckDBStore(Settings().duckdb_path)


@st.cache_data(ttl=300)
def load_summary() -> pd.DataFrame:
    return get_store().summary()


@st.cache_data(ttl=300)
def load_prices(zones: tuple[str, ...], start: date, end: date) -> pd.DataFrame:
    df = get_store().query_prices(zones=list(zones), start=start, end=end)
    if df.empty:
        return df
    df["time_start"] = pd.to_datetime(df["time_start"], utc=True)
    df["time_utc"] = df["time_start"].dt.tz_convert("UTC")
    df["hour"] = df["time_utc"].dt.hour
    df["day_of_week"] = df["time_utc"].dt.day_name()
    df["date"] = df["time_utc"].dt.date
    df["zone_label"] = df["zone"].map(ZONE_NAMES)
    return df


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.image(
    "https://flagcdn.com/w80/no.png",
    width=48,
)
st.sidebar.title("Nordic Power Terminal")
st.sidebar.caption("Hourly spot prices · NO1–NO5 · Nord Pool")
st.sidebar.divider()

summary = load_summary()

if summary.empty:
    st.error(
        "**No data found.** Run `npt ingest-prices --start YYYY-MM-DD --end YYYY-MM-DD` "
        "from the terminal to populate the database, then refresh this page."
    )
    st.stop()

db_min = pd.to_datetime(summary["first_obs"]).min().date()
db_max = pd.to_datetime(summary["last_obs"]).max().date()

st.sidebar.markdown(f"**Data available:** {db_min} → {db_max}")
st.sidebar.divider()

selected_zones = st.sidebar.multiselect(
    "Bidding zones",
    options=["NO1", "NO2", "NO3", "NO4", "NO5"],
    default=["NO1", "NO2", "NO3", "NO4", "NO5"],
    format_func=lambda z: ZONE_NAMES[z],
)

# Default end to the last date all selected zones share data
sel_summary = summary[summary["zone"].isin(selected_zones)]
if not sel_summary.empty:
    default_end = pd.to_datetime(sel_summary["last_obs"]).min().date()
else:
    default_end = db_max

default_start = max(db_min, default_end - timedelta(days=90))
date_start = st.sidebar.date_input("From", value=default_start, min_value=db_min, max_value=db_max)
date_end = st.sidebar.date_input("To", value=default_end, min_value=db_min, max_value=db_max)

if date_start > date_end:
    st.sidebar.error("'From' must be before 'To'.")
    st.stop()

if not selected_zones:
    st.sidebar.warning("Select at least one zone.")
    st.stop()

st.sidebar.divider()
st.sidebar.caption("© Birk Tyssebotn · MIT Licence")

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

with st.spinner("Loading prices…"):
    df = load_prices(tuple(selected_zones), date_start, date_end)

if df.empty:
    st.warning("No price data for the selected zones and date range.")
    st.stop()

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------

st.markdown("## Nordic Power Terminal")

cols = st.columns(len(selected_zones))
for col, zone in zip(cols, selected_zones):
    zone_df = df[df["zone"] == zone]
    if zone_df.empty:
        col.metric(label=ZONE_NAMES[zone], value="No data")
        continue
    latest = zone_df.sort_values("time_utc").iloc[-1]
    avg = zone_df["nok_per_kwh"].mean()
    col.metric(
        label=ZONE_NAMES[zone],
        value=f"{latest['nok_per_kwh']:.4f} NOK/kWh",
        delta=f"avg {avg:.4f}",
        delta_color="off",
    )

st.divider()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_prices, tab_analytics, tab_backtest, tab_data = st.tabs(
    ["Prices", "Analytics", "Backtest", "Raw data"]
)

# ── Tab 1: Prices ──────────────────────────────────────────────────────────

with tab_prices:
    st.subheader("Hourly spot prices")

    daily = (
        df.groupby(["date", "zone", "zone_label"])["nok_per_kwh"]
        .mean()
        .reset_index()
        .rename(columns={"nok_per_kwh": "avg_nok_per_kwh"})
    )

    granularity = st.radio(
        "Granularity", ["Hourly", "Daily average"], horizontal=True, index=1
    )

    if granularity == "Hourly":
        plot_df = df.rename(columns={"time_utc": "Time", "nok_per_kwh": "NOK/kWh"})
        fig = px.line(
            plot_df,
            x="Time",
            y="NOK/kWh",
            color="zone_label",
            color_discrete_map={v: ZONE_COLOURS[k] for k, v in ZONE_NAMES.items()},
            labels={"zone_label": "Zone"},
        )
    else:
        plot_df = daily.rename(columns={"date": "Date", "avg_nok_per_kwh": "Avg NOK/kWh"})
        fig = px.line(
            plot_df,
            x="Date",
            y="Avg NOK/kWh",
            color="zone_label",
            color_discrete_map={v: ZONE_COLOURS[k] for k, v in ZONE_NAMES.items()},
            labels={"zone_label": "Zone"},
        )

    fig.update_layout(
        legend_title_text="Zone",
        hovermode="x unified",
        margin=dict(t=20, b=20),
        height=420,
    )
    st.plotly_chart(fig, width="stretch")

    # Zone spread
    if len(selected_zones) > 1:
        st.subheader("Zone price spread (daily avg)")
        pivot = daily.pivot(index="date", columns="zone", values="avg_nok_per_kwh")
        spread = pivot.max(axis=1) - pivot.min(axis=1)
        spread_df = spread.reset_index().rename(columns={0: "Spread (NOK/kWh)", "date": "Date"})
        fig2 = px.area(
            spread_df,
            x="Date",
            y="Spread (NOK/kWh)",
            color_discrete_sequence=["#8172B2"],
        )
        fig2.update_layout(margin=dict(t=10, b=20), height=250)
        st.plotly_chart(fig2, width="stretch")

# ── Tab 2: Analytics ───────────────────────────────────────────────────────

with tab_analytics:
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Avg price by hour of day")
        hourly_avg = (
            df.groupby(["zone_label", "hour"])["nok_per_kwh"]
            .mean()
            .reset_index()
            .rename(columns={"nok_per_kwh": "Avg NOK/kWh", "hour": "Hour (UTC)"})
        )
        fig3 = px.line(
            hourly_avg,
            x="Hour (UTC)",
            y="Avg NOK/kWh",
            color="zone_label",
            color_discrete_map={v: ZONE_COLOURS[k] for k, v in ZONE_NAMES.items()},
            labels={"zone_label": "Zone"},
            markers=True,
        )
        fig3.update_layout(margin=dict(t=10, b=10), height=340)
        st.plotly_chart(fig3, width="stretch")

    with col_right:
        st.subheader("Price distribution by zone")
        fig4 = px.box(
            df,
            x="zone_label",
            y="nok_per_kwh",
            color="zone_label",
            color_discrete_map={v: ZONE_COLOURS[k] for k, v in ZONE_NAMES.items()},
            labels={"nok_per_kwh": "NOK/kWh", "zone_label": "Zone"},
        )
        fig4.update_layout(showlegend=False, margin=dict(t=10, b=10), height=340)
        st.plotly_chart(fig4, width="stretch")

    st.subheader("Weekly heatmap — avg price by hour and day")
    heatmap_zone = st.selectbox(
        "Zone", options=selected_zones, format_func=lambda z: ZONE_NAMES[z], key="heatmap_zone"
    )
    DOW_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    heat_df = (
        df[df["zone"] == heatmap_zone]
        .groupby(["day_of_week", "hour"])["nok_per_kwh"]
        .mean()
        .reset_index()
    )
    heat_pivot = heat_df.pivot(index="day_of_week", columns="hour", values="nok_per_kwh")
    heat_pivot = heat_pivot.reindex([d for d in DOW_ORDER if d in heat_pivot.index])

    fig5 = px.imshow(
        heat_pivot,
        labels=dict(x="Hour (UTC)", y="", color="NOK/kWh"),
        color_continuous_scale="RdYlGn_r",
        aspect="auto",
    )
    fig5.update_layout(margin=dict(t=10, b=10), height=280)
    st.plotly_chart(fig5, width="stretch")

    st.subheader("Descriptive statistics")
    stats = (
        df.groupby("zone")["nok_per_kwh"]
        .agg(
            Count="count",
            Mean="mean",
            Std="std",
            Min="min",
            P25=lambda x: x.quantile(0.25),
            Median="median",
            P75=lambda x: x.quantile(0.75),
            Max="max",
        )
        .round(4)
        .reset_index()
    )
    st.dataframe(stats, width="stretch", hide_index=True)

# ── Tab 3: Backtest ────────────────────────────────────────────────────────

with tab_backtest:
    st.subheader("Seasonal-naive walk-forward backtest")
    st.caption(
        "Forecast: price at hour t = price at hour t minus 168 (same hour, 7 days prior). "
        "Evaluated one day ahead at a time with no look-ahead."
    )

    bt_zone = st.selectbox(
        "Zone to backtest", options=selected_zones, format_func=lambda z: ZONE_NAMES[z]
    )

    run_bt = st.button("Run backtest", type="primary")

    if run_bt:
        from npt.backtest.walk_forward import mae_rmse, walk_forward_daily_seasonal_naive

        bt_df = df[df["zone"] == bt_zone].set_index("time_utc").sort_index()

        with st.spinner("Running walk-forward evaluation…"):
            result = walk_forward_daily_seasonal_naive(bt_df)

        if result.preds.empty:
            st.warning("Not enough data — need at least 8 days of hourly observations.")
        else:
            mae, rmse = mae_rmse(result.preds)
            n_days = len(result.preds) // 24

            k1, k2, k3 = st.columns(3)
            k1.metric("Forecast days", n_days)
            k2.metric("MAE (NOK/kWh)", f"{mae:.5f}")
            k3.metric("RMSE (NOK/kWh)", f"{rmse:.5f}")

            preds_plot = result.preds.copy().reset_index()
            preds_plot.columns = ["Time", "Actual", "Forecast"]
            preds_plot = preds_plot.tail(24 * 14)  # last 2 weeks for readability

            fig6 = go.Figure()
            fig6.add_trace(
                go.Scatter(
                    x=preds_plot["Time"],
                    y=preds_plot["Actual"],
                    name="Actual",
                    line=dict(color=ZONE_COLOURS.get(bt_zone, "#4C72B0"), width=1.5),
                )
            )
            fig6.add_trace(
                go.Scatter(
                    x=preds_plot["Time"],
                    y=preds_plot["Forecast"],
                    name="Forecast (seasonal naive)",
                    line=dict(color="#aaaaaa", width=1.5, dash="dash"),
                )
            )
            fig6.update_layout(
                hovermode="x unified",
                legend_title_text="",
                yaxis_title="NOK/kWh",
                margin=dict(t=10, b=20),
                height=400,
                title="Actual vs forecast — last 14 days of evaluation window",
            )
            st.plotly_chart(fig6, width="stretch")

            # Residuals
            preds_all = result.preds.copy()
            preds_all["residual"] = preds_all["y"] - preds_all["yhat"]
            fig7 = px.histogram(
                preds_all,
                x="residual",
                nbins=60,
                labels={"residual": "Residual (NOK/kWh)"},
                title="Forecast error distribution",
                color_discrete_sequence=[ZONE_COLOURS.get(bt_zone, "#4C72B0")],
            )
            fig7.update_layout(margin=dict(t=40, b=20), height=280)
            st.plotly_chart(fig7, width="stretch")
    else:
        st.info("Select a zone and click **Run backtest** to evaluate the model.")

# ── Tab 4: Raw data ────────────────────────────────────────────────────────

with tab_data:
    st.subheader("Raw price data")

    display_df = (
        df[["zone", "time_utc", "nok_per_kwh", "eur_per_kwh", "exr", "source"]]
        .rename(
            columns={
                "zone": "Zone",
                "time_utc": "Time (UTC)",
                "nok_per_kwh": "NOK/kWh",
                "eur_per_kwh": "EUR/kWh",
                "exr": "EXR",
                "source": "Source",
            }
        )
        .sort_values("Time (UTC)", ascending=False)
        .reset_index(drop=True)
    )

    st.caption(f"{len(display_df):,} rows · {date_start} → {date_end} · zones: {', '.join(selected_zones)}")
    st.dataframe(display_df, width="stretch", height=480)

    csv = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download as CSV",
        data=csv,
        file_name=f"npt_prices_{'_'.join(selected_zones)}_{date_start}_{date_end}.csv",
        mime="text/csv",
    )

    st.divider()
    st.subheader("Database summary")
    st.dataframe(summary, width="stretch", hide_index=True)
