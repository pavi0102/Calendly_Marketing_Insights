"""
Calendly Marketing Insights Dashboard
Reads the batch Gold Delta tables (bookings_by_source_daily, cpb_by_channel,
booking_time_distribution, employee_meeting_load) from S3, and the live
speed layer data from DynamoDB, then merges them into one dashboard.

Defensive by design: every table load and every column access checks for
missing data (empty tables, missing columns, null values) before using it,
so a partially-populated pipeline degrades gracefully instead of crashing.

Credentials are loaded from credentials.env (never hardcoded, never committed).
"""

import os
import boto3
import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
from deltalake import DeltaTable

# ---------- CREDENTIALS ----------
load_dotenv("credentials.env")

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
    "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    "AWS_REGION": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
}

BUCKET = os.environ.get("GOLD_BUCKET", "calendly-marketing-insights-ps")
GOLD_PATH = f"s3://{BUCKET}/gold"
DYNAMODB_TABLE_NAME = os.environ.get("SPEED_TABLE_NAME", "calendly_speed_bookings")

st.set_page_config(page_title="Calendly Marketing Insights", layout="wide")


# ---------- BATCH (GOLD) DATA LOADING ----------
@st.cache_data(ttl=600)
def load_gold_table(table_name: str, expected_columns: list) -> pd.DataFrame:
    """
    Loads a Gold Delta table. If it's empty or missing, returns an empty
    DataFrame with the expected columns already present, so downstream
    code can safely reference those columns without a KeyError.
    """
    path = f"{GOLD_PATH}/{table_name}"
    try:
        dt = DeltaTable(path, storage_options=STORAGE_OPTIONS)
        df = dt.to_pandas()
    except Exception as e:
        st.warning(f"Could not load Gold table '{table_name}': {e}")
        return pd.DataFrame(columns=expected_columns)

    if df.empty:
        return pd.DataFrame(columns=expected_columns)

    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.NA

    return df


# ---------- SPEED (DYNAMODB) DATA LOADING ----------
@st.cache_data(ttl=60)  # short TTL — this is the "live" side of the dashboard
def load_speed_table() -> pd.DataFrame:
    """
    Scans the DynamoDB speed table. A full scan is fine at this data
    volume (invitee.created events only, single day's worth of live
    bookings) — would need a query pattern with a GSI instead if this
    table's item count grows meaningfully. Always returns a DataFrame
    with every expected column present, even when empty.
    """
    expected_columns = [
        "booking_id", "booking_date", "channel", "utm_source",
        "host_email", "host_name", "start_time", "end_time",
    ]

    try:
        dynamodb = boto3.resource(
            "dynamodb",
            aws_access_key_id=STORAGE_OPTIONS["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=STORAGE_OPTIONS["AWS_SECRET_ACCESS_KEY"],
            region_name=STORAGE_OPTIONS["AWS_REGION"],
        )
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)

        items = []
        response = table.scan()
        items.extend(response.get("Items", []))
        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response.get("Items", []))
    except Exception as e:
        st.warning(f"Could not load live data from DynamoDB: {e}")
        items = []

    if not items:
        return pd.DataFrame(columns=expected_columns)

    df = pd.DataFrame(items)
    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.NA

    return df


# ---------- LOAD EVERYTHING ----------
bookings_by_source_daily = load_gold_table(
    "bookings_by_source_daily", ["booking_date", "utm_source", "booking_count"]
)
cpb_by_channel = load_gold_table(
    "cpb_by_channel", ["channel", "total_bookings", "total_spend", "cost_per_booking"]
)
booking_time_distribution = load_gold_table(
    "booking_time_distribution", ["day_of_week", "booking_date", "utm_source", "booking_count"]
)
employee_meeting_load = load_gold_table(
    "employee_meeting_load", ["host_email", "week_start_date", "meeting_count"]
)
speed_df = load_speed_table()

# --- normalize dates defensively; bad/missing values become NaT, then get dropped ---
bookings_by_source_daily["booking_date"] = pd.to_datetime(
    bookings_by_source_daily["booking_date"], errors="coerce"
)
bookings_by_source_daily = bookings_by_source_daily.dropna(subset=["booking_date"])

booking_time_distribution["booking_date"] = pd.to_datetime(
    booking_time_distribution["booking_date"], errors="coerce"
)
booking_time_distribution = booking_time_distribution.dropna(subset=["booking_date"])

employee_meeting_load["week_start_date"] = pd.to_datetime(
    employee_meeting_load["week_start_date"], errors="coerce"
)

if not speed_df.empty:
    speed_df["booking_date"] = pd.to_datetime(speed_df["booking_date"], errors="coerce")

# --- fill missing categorical values so they're visible/filterable rather than silently dropped ---
bookings_by_source_daily["utm_source"] = bookings_by_source_daily["utm_source"].fillna("(no utm_source)")
booking_time_distribution["utm_source"] = booking_time_distribution["utm_source"].fillna("(no utm_source)")

st.title("Calendly Marketing Insights")

if bookings_by_source_daily.empty:
    st.warning("No batch (Gold) booking data available yet.")
    batch_through = "N/A"
else:
    batch_through = bookings_by_source_daily["booking_date"].max().date()

st.caption(
    f"Batch data through {batch_through} "
    f"· Live data refreshed every 60s from DynamoDB ({len(speed_df)} bookings today)"
)

# ---------- SIDEBAR FILTERS ----------
st.sidebar.header("Filters")

today = pd.Timestamp.today().normalize()

if bookings_by_source_daily.empty:
    min_date = today
    max_date = today
else:
    min_date = bookings_by_source_daily["booking_date"].min()
    max_date = max(bookings_by_source_daily["booking_date"].max(), today)

date_range = st.sidebar.date_input(
    "Booking date range", value=(min_date, max_date), min_value=min_date, max_value=max_date
)
start_date, end_date = date_range if len(date_range) == 2 else (min_date, max_date)

sources = sorted(bookings_by_source_daily["utm_source"].dropna().unique().tolist())
selected_sources = st.sidebar.multiselect("Source (UTM)", sources, default=sources)

channels = sorted(cpb_by_channel["channel"].dropna().unique().tolist())
selected_channels = st.sidebar.multiselect("Channel", channels, default=channels)

# ---------- MERGE SPEED INTO THE DAILY BOOKINGS VIEW ----------
# Only add speed rows for dates after the batch layer's last processed
# date, so a booking that's already in Gold doesn't get double-counted
# once the next batch run picks it up.
gold_max_date = bookings_by_source_daily["booking_date"].max() if not bookings_by_source_daily.empty else pd.Timestamp.min

if not speed_df.empty and speed_df["booking_date"].notna().any():
    speed_daily = (
        speed_df[speed_df["booking_date"] > gold_max_date]
        .groupby(["booking_date", "utm_source"])
        .size()
        .reset_index(name="booking_count")
    )
    combined_daily = pd.concat([bookings_by_source_daily, speed_daily], ignore_index=True)
else:
    combined_daily = bookings_by_source_daily.copy()

if combined_daily.empty or not selected_sources:
    daily_filtered = pd.DataFrame(columns=["booking_date", "utm_source", "booking_count"])
else:
    daily_filtered = combined_daily[
        (combined_daily["booking_date"] >= pd.to_datetime(start_date))
        & (combined_daily["booking_date"] <= pd.to_datetime(end_date))
        & (combined_daily["utm_source"].isin(selected_sources))
    ]

if booking_time_distribution.empty or not selected_sources:
    time_dist_filtered = pd.DataFrame(columns=["day_of_week", "booking_date", "utm_source", "booking_count"])
else:
    time_dist_filtered = booking_time_distribution[
        (booking_time_distribution["booking_date"] >= pd.to_datetime(start_date))
        & (booking_time_distribution["booking_date"] <= pd.to_datetime(end_date))
        & (booking_time_distribution["utm_source"].isin(selected_sources))
    ]

if cpb_by_channel.empty or not selected_channels:
    cpb_filtered = pd.DataFrame(columns=["channel", "total_bookings", "total_spend", "cost_per_booking"])
else:
    cpb_filtered = cpb_by_channel[cpb_by_channel["channel"].isin(selected_channels)]

# ---------- KPI SUMMARY ----------
total_bookings = int(daily_filtered["booking_count"].sum()) if not daily_filtered.empty else 0
live_bookings_today = len(speed_df) if not speed_df.empty else 0
total_spend = float(cpb_filtered["total_spend"].sum()) if not cpb_filtered.empty else 0.0
avg_cpb = (total_spend / total_bookings) if total_bookings > 0 else 0.0
active_hosts = employee_meeting_load["host_email"].nunique() if not employee_meeting_load.empty else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Bookings", f"{total_bookings:,}")
k2.metric("Live Bookings (Today)", live_bookings_today)
k3.metric("Total Spend", f"${total_spend:,.2f}")
k4.metric("Blended Cost per Booking", f"${avg_cpb:,.2f}")
k5.metric("Active Hosts", active_hosts)

st.divider()

# ---------- LIVE BOOKINGS (SPEED LAYER) ----------
st.subheader("Live Bookings — Today")
if speed_df.empty:
    st.info("No meetings booked today for channels facebook_paid_ads, youtube_paid_ads, or tiktok_paid_ads.")
else:
    live_col1, live_col2 = st.columns([1, 2])

    with live_col1:
        if speed_df["channel"].notna().any():
            live_by_channel = speed_df["channel"].dropna().value_counts().reset_index()
            live_by_channel.columns = ["channel", "count"]
            fig_live = px.pie(live_by_channel, names="channel", values="count", hole=0.4)
            st.plotly_chart(fig_live, use_container_width=True)
        else:
            st.info("No meetings booked today for channels facebook_paid_ads, youtube_paid_ads, or tiktok_paid_ads.")

    with live_col2:
        st.caption("Most recent bookings (live, not yet in the batch Gold layer)")
        display_cols = [c for c in ["booking_id", "channel", "utm_source", "host_name", "start_time"] if c in speed_df.columns]
        sort_col = "start_time" if "start_time" in speed_df.columns and speed_df["start_time"].notna().any() else None
        table_to_show = speed_df[display_cols]
        if sort_col:
            table_to_show = table_to_show.sort_values(sort_col, ascending=False)
        st.dataframe(table_to_show.head(10), use_container_width=True, hide_index=True)

st.caption(
    "Note: cost-per-booking and channel attribution below reflect only "
    "the batch layer — the marketing spend file is loaded once daily for "
    "the prior day, so today's live bookings don't yet have a matching "
    "spend figure to compute CPB against."
)

st.divider()

# ---------- 1.1 / 1.3: Bookings over time by source (batch + live combined) ----------
st.subheader("Bookings Over Time by Source")
if daily_filtered.empty:
    st.info("No booking data available for the selected filters.")
else:
    fig_trend = px.line(
        daily_filtered.sort_values("booking_date"),
        x="booking_date",
        y="booking_count",
        color="utm_source",
        markers=True,
    )
    st.plotly_chart(fig_trend, use_container_width=True)

st.divider()

# ---------- 1.2 / 1.4: Cost per booking + channel attribution leaderboard (batch only) ----------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Cost per Booking (CPB) by Channel")
    if cpb_filtered.empty:
        st.info("No channel spend data available for the selected filters.")
    else:
        fig_cpb = px.bar(
            cpb_filtered.sort_values("cost_per_booking"),
            x="cost_per_booking",
            y="channel",
            orientation="h",
            text="cost_per_booking",
        )
        fig_cpb.update_traces(texttemplate="$%{text:.2f}", textposition="outside")
        st.plotly_chart(fig_cpb, use_container_width=True)

with col2:
    st.subheader("Channel Attribution Leaderboard")
    if cpb_filtered.empty:
        st.info("No channel attribution data available for the selected filters.")
    else:
        leaderboard = cpb_filtered.sort_values("total_bookings", ascending=False)[
            ["channel", "total_bookings", "total_spend", "cost_per_booking"]
        ]
        st.dataframe(
            leaderboard.style.format(
                {"total_spend": "${:,.2f}", "cost_per_booking": "${:,.2f}"}
            ),
            use_container_width=True,
            hide_index=True,
        )

st.divider()

# ---------- 1.5: Booking volume by day of week (batch only — needs full-day data) ----------
st.subheader("Booking Volume by Day of Week")
if time_dist_filtered.empty:
    st.info("No time-distribution data available for the selected filters.")
else:
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    heatmap_data = (
        time_dist_filtered.groupby(["day_of_week", "utm_source"])["booking_count"]
        .sum()
        .reset_index()
        .pivot(index="day_of_week", columns="utm_source", values="booking_count")
        .reindex(day_order)
        .fillna(0)
    )
    fig_heatmap = px.imshow(
        heatmap_data,
        text_auto=True,
        aspect="auto",
        color_continuous_scale="Blues",
        labels=dict(x="Source", y="Day of Week", color="Bookings"),
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)

st.divider()

# ---------- 1.6: Employee meeting load (batch + today's live meetings) ----------
st.subheader("Employee Meeting Load per Week")

meeting_load = employee_meeting_load.copy()

if not speed_df.empty and speed_df["host_email"].notna().any():
    current_week_start = (today - pd.to_timedelta(today.dayofweek, unit="d")).normalize()
    live_meetings_by_host = (
        speed_df[speed_df["host_email"].notna()]
        .groupby("host_email")
        .size()
        .reset_index(name="meeting_count")
    )
    live_meetings_by_host["week_start_date"] = current_week_start
    meeting_load = pd.concat([meeting_load, live_meetings_by_host], ignore_index=True)

if meeting_load.empty or meeting_load["host_email"].dropna().empty:
    st.info("No employee meeting load data available yet.")
else:
    host_avg = (
        meeting_load.groupby("host_email")["meeting_count"]
        .agg(total_meetings="sum", weeks_active="count")
        .reset_index()
    )
    host_avg["avg_meetings_per_week"] = host_avg["total_meetings"] / host_avg["weeks_active"]

    selected_host = st.selectbox(
        "View weekly trend for host", ["All hosts"] + sorted(meeting_load["host_email"].dropna().unique().tolist())
    )

    if selected_host == "All hosts":
        trend_df = meeting_load.groupby("week_start_date")["meeting_count"].sum().reset_index()
    else:
        trend_df = meeting_load[meeting_load["host_email"] == selected_host].sort_values("week_start_date")

    fig_load = px.bar(trend_df, x="week_start_date", y="meeting_count")
    st.plotly_chart(fig_load, use_container_width=True)

    st.caption("Average meetings per week, by host (includes today's live bookings in the current week)")
    st.dataframe(
        host_avg.sort_values("avg_meetings_per_week", ascending=False)[
            ["host_email", "avg_meetings_per_week", "total_meetings"]
        ].style.format({"avg_meetings_per_week": "{:.1f}"}),
        use_container_width=True,
        hide_index=True,
    )