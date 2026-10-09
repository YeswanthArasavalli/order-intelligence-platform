"""M6 Streamlit consumer of M5 snapshots with authoritative M4 SQL fallback.

From the project root, with environment loaded:
    python -m streamlit run dashboard/app.py
"""

import atexit
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.read_service import (
    DashboardConfig, DashboardReadService, DashboardUnavailable, display_rows,
    filter_rows, format_money,
)


@st.cache_resource
def get_read_service(config):
    service = DashboardReadService(config)
    atexit.register(service.close)
    return service


def revenue_chart(rows, currency):
    """Only chart coordinates are approximate floats; exact Decimal text is retained."""
    figure = go.Figure()
    for region in sorted({row["region"] for row in rows}):
        selected = [r for r in rows if r["region"] == region]
        figure.add_trace(go.Scatter(
            x=[r["window_start"].isoformat() for r in selected],
            y=[float(r["net_revenue"]) for r in selected],  # presentation-only Plotly boundary
            mode="lines+markers", name=f"{region} / {currency}",
            customdata=[[format(r["net_revenue"], ".8f"), r["window_end"].isoformat()] for r in selected],
            hovertemplate="%{x}<br>Window end: %{customdata[1]}<br>Net: %{customdata[0]} " + currency + "<extra>%{fullData.name}</extra>",
        ))
    figure.update_layout(yaxis_title=f"Net revenue — {currency}", xaxis_title="Window start (UTC)",
                         height=300, margin=dict(l=0, r=0, t=15, b=0))
    return figure


def render(config):
    st.button("Refresh snapshot", key="refresh")
    try:
        snapshot = get_read_service(config).load()
    except DashboardUnavailable as exc:
        st.error(str(exc))
        st.info("No analytical values are displayed. Retry when a valid source is available.")
        return

    st.caption(f"Data source: {snapshot.source} | Redis status: {snapshot.redis_status}")
    st.caption(f"PostgreSQL source: {snapshot.metadata['source_schema']} | Gold version: {snapshot.metadata['gold_version']} | "
               f"Source topic: {snapshot.metadata['source_topic']} | Status: {snapshot.metadata['serving_status']}")
    st.caption(f"PostgreSQL served_at: {snapshot.metadata['served_at']}")
    if snapshot.source == "Redis cache":
        st.info("Showing the cached source version. Current PostgreSQL freshness/availability has not been checked.")
    else:
        st.warning(f"Redis {snapshot.redis_status}; showing the authoritative PostgreSQL snapshot read for this refresh.")
    with st.expander("Snapshot provenance"):
        st.json(snapshot.provenance)
    if snapshot.empty:
        st.info("Valid empty committed snapshot: zero revenue and product rows.")
        return

    currency_options = ["All currencies (separate views)", *snapshot.currencies]
    default_index = currency_options.index(config.default_currency) if config.default_currency in snapshot.currencies else 0
    if config.default_currency and default_index == 0:
        st.warning(f"Configured currency {config.default_currency} is absent; showing separate currency views.")
    currency = st.selectbox("Currency", currency_options, index=default_index, key="currency")
    region = st.selectbox("Revenue region", [None, *sorted({r['region'] for r in snapshot.revenue_by_minute})],
                          format_func=lambda value: "All regions" if value is None else value, key="region")
    st.caption("Region filters revenue only. Product snapshots have no region dimension.")
    period = st.selectbox("Time range", ["All snapshot windows", "Last 30 minutes (UTC now)", "Custom UTC range"], key="period")
    start = end = None
    if period == "Last 30 minutes (UTC now)":
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=30)
    elif period == "Custom UTC range":
        combined = (*snapshot.revenue_by_minute, *snapshot.product_rankings)
        first = min(r["window_start"] for r in combined).isoformat()
        last = max(r["window_end"] for r in combined).isoformat()
        try:
            start = datetime.fromisoformat(st.text_input("Start (inclusive, timezone required)", first, key="start"))
            end = datetime.fromisoformat(st.text_input("End (exclusive, timezone required)", last, key="end"))
        except ValueError:
            st.error("Enter ISO-8601 timestamps with a timezone.")
            return
    try:
        filtered = filter_rows(snapshot, currency=None if currency == currency_options[0] else currency,
                               region=region, start=start, end=end)
    except ValueError as exc:
        st.error(str(exc))
        return
    st.caption("Time filtering selects stored window_start in [start, end); windows are not prorated.")
    currencies = snapshot.currencies if currency == currency_options[0] else [currency]
    for code in currencies:
        st.divider()
        st.subheader(f"Revenue — {code}")
        revenue = [r for r in filtered["revenue_by_minute"] if r["currency"] == code]
        if revenue:
            index = st.selectbox("KPI source row (one stored window/region)", range(len(revenue)),
                                 index=len(revenue) - 1,
                                 format_func=lambda i, rows=revenue, currency=code: f"{rows[i]['window_start'].isoformat()} — "
                                                       f"{rows[i]['window_end'].isoformat()} / {rows[i]['region']} / {currency}",
                                 key="kpi_" + code)
            row = revenue[index]
            a, b, c = st.columns(3)
            a.metric(f"Net revenue — {code}", format_money(row["net_revenue"], code))
            b.metric(f"Orders — {code}", str(row["order_count"]))
            c.metric(f"Average order value — {code}", format_money(row["avg_order_value"], code))
            st.plotly_chart(revenue_chart(revenue, code), use_container_width=True)
            st.caption("Plot positions are approximate; hover text and the table retain exact eight-place amounts.")
            st.dataframe(pd.DataFrame(display_rows(revenue)), use_container_width=True, hide_index=True)
        else:
            st.info(f"No revenue rows match these filters for {code}; this is not a zero-revenue assertion.")
        st.subheader(f"Products — {code}")
        products = [r for r in filtered["product_rankings"] if r["currency"] == code]
        if products:
            st.caption("Stored product/window rows; IDs, names and window boundaries remain distinct.")
            st.dataframe(pd.DataFrame(display_rows(products)), use_container_width=True, hide_index=True)
        else:
            st.info(f"No product rows match these filters for {code}.")


def main():
    st.set_page_config(page_title="Order Intelligence Platform", page_icon="📦", layout="wide")
    st.title("📦 Order Intelligence Platform")
    try:
        config = DashboardConfig.from_env()
    except ValueError:
        st.error("Invalid dashboard configuration. Check source schema, Redis namespace, currency and refresh interval.")
        return
    st.caption(f"Snapshot display refresh: {config.refresh_seconds}s. PostgreSQL is authoritative; Redis is a disposable cache.")
    # Streamlit fragment refresh replaces the previous blocking sleep/rerun loop.
    st.fragment(run_every=config.refresh_seconds)(render)(config)


if __name__ == "__main__":
    main()
