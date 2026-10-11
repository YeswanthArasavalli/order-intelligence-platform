"""M6 Streamlit consumer of M5 snapshots with authoritative M4 SQL fallback.

From the project root, with environment loaded:
    python -m streamlit run dashboard/app.py

With ``OIP_DEMO_MODE=synthetic`` the same page renders the fixed synthetic demo
sample instead; see ``dashboard/demo_provider.py`` and ``python -m dashboard.demo``.
With ``OIP_DEMO_MODE=scripted`` it renders the scripted synthetic replay: a
finite predefined snapshot sequence that advances on the refresh clock; see
``dashboard/replay.py``.
"""

import atexit
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, localcontext

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import replay as replay_provider
from dashboard.demo_provider import (DEMO_DISCLOSURE, MODE_DEMO, MODE_INVALID, MODE_REAL, MODE_REPLAY, demo_config,
                                     mode_error_message, selected_mode, synthetic_snapshot)
from dashboard.read_service import (
    DashboardConfig, DashboardReadService, DashboardUnavailable, display_rows, filter_rows,
    format_amount, format_money,
)
from storage.postgres_redis_serving import TABLES

# Real mode keeps its historical options. The demo replaces the clock-based
# option: the fixed sample is anchored in the past, so "now" filtering would
# legitimately select zero rows and the clock is not part of the demo output.
REAL_PERIOD_OPTIONS = ["All snapshot windows", "Last 30 minutes (UTC now)", "Custom UTC range"]
DEMO_PERIOD_OPTIONS = ["All synthetic sample windows", "Custom UTC range"]

# One-line framing under the title, per mode, so a first-time reader knows in
# seconds what this page is and where the numbers come from.
MODE_SUBTITLES = {
    MODE_REAL: "Real-time order analytics: the streaming pipeline serves these figures from PostgreSQL/Redis.",
    MODE_DEMO: "Interactive order-analytics dashboard, demonstrated on a fixed synthetic sample.",
    MODE_REPLAY: "Interactive order-analytics dashboard, demonstrated on a scripted sequence of synthetic snapshots.",
}


@st.cache_resource
def get_read_service(config):
    service = DashboardReadService(config)
    atexit.register(service.close)
    return service


def revenue_chart(rows, currency):
    """Only chart coordinates are approximate floats; hover text uses the shared money formatter."""
    figure = go.Figure()
    for region in sorted({row["region"] for row in rows}):
        selected = [r for r in rows if r["region"] == region]
        figure.add_trace(go.Scatter(
            x=[r["window_start"].isoformat() for r in selected],
            y=[float(r["net_revenue"]) for r in selected],  # presentation-only Plotly boundary
            mode="lines+markers", name=f"{region} / {currency}",
            customdata=[[format_amount(r["net_revenue"]), r["window_end"].isoformat()] for r in selected],
            hovertemplate="%{x}<br>Window end: %{customdata[1]}<br>Net: %{customdata[0]} " + currency + "<extra>%{fullData.name}</extra>",
        ))
    figure.update_layout(yaxis_title=f"Net revenue — {currency}", xaxis_title="Window start (UTC)",
                         height=300, margin=dict(l=0, r=0, t=15, b=0))
    return figure


def currency_insights(revenue, products, currency, region=None):
    """Phase 2C P3: currency-scoped insight lines recomputed from the filtered rows.

    Money is never summed across currencies and every figure uses the same exact
    Decimal arithmetic the tables render with, so an insight can never disagree
    with a number shown above it. An empty selection simply yields no lines.

    Phase 2C fixes: ``order_count``/``units_sold`` are PAID-EVENT counts
    (GOLD_CONTRACT), not distinct orders or physical quantities, so the labels say
    "events". The product schema has no region dimension, so product lines are
    always all-region while revenue lines honour the active ``region`` filter; the
    two scopes are labelled separately and never implied to coincide. Leading-region
    ties break deterministically on the ascending region name.
    """
    lines = []
    revenue_scope = f"region {region}" if region else "all regions"
    if revenue:
        regions = {}
        for row in revenue:
            regions[row["region"]] = regions.get(row["region"], Decimal(0)) + row["net_revenue"]
        lead_region, lead_net = min(regions.items(), key=lambda item: (-item[1], item[0]))
        lines.append((f"Leading region by net revenue ({len(regions)} in view, {revenue_scope})",
                      f"{lead_region} — {format_money(lead_net, currency)}"))
        lines.append((f"Paid events in view (revenue rows, {revenue_scope})", str(sum(row["order_count"] for row in revenue))))
        windows = sorted({row["window_start"] for row in revenue})
        if len(windows) >= 2:
            first_net = sum((r["net_revenue"] for r in revenue if r["window_start"] == windows[0]), Decimal(0))
            last_net = sum((r["net_revenue"] for r in revenue if r["window_start"] == windows[-1]), Decimal(0))
            delta = last_net - first_net
            trend = "rising" if delta > 0 else "falling" if delta < 0 else "flat"
            span = f"{windows[0].strftime('%H:%M')}\u2013{windows[-1].strftime('%H:%M')} UTC"
            lines.append((f"Net revenue {trend} across {len(windows)} windows ({span})",
                          f"first\u2192last change {format_money(delta, currency)}"))
        gross = sum((row["gross_revenue"] for row in revenue), Decimal(0))
        deducted = sum((row["refund_amount"] + row["cancellation_amount"] for row in revenue), Decimal(0))
        if gross > 0:
            with localcontext() as ctx:
                ctx.prec = 80
                rate = (deducted / gross * 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            lines.append(("Combined refund + cancellation rate over gross paid (additive ledger)",
                          f"{rate}% of gross ({format_money(deducted, currency)}) (reversals are an additive "
                          "event ledger; a paid event may be both refunded and cancelled, so this ratio can "
                          "exceed 100%"))
    if products:
        top = max(products, key=lambda r: (r["revenue"], r["product_id"]))
        lines.append(("Top product by revenue (all regions)",
                      f"{top['product_name']} ({top['product_id']}) \u2014 {format_money(top['revenue'], currency)}, "
                      f"{top['units_sold']} paid events"))
        lines.append((f"Paid product events in view (all regions, {len(products)} product rows)", str(sum(row["units_sold"] for row in products))))
    return lines


def display_frame(rows, table):
    """Render a table in the M4/M5 contract column order instead of alphabetical.

    ``pd.DataFrame(list_of_dicts)`` sorts the union of keys alphabetically, which
    pushed the two wide window-boundary timestamps to the far right so the 1440px
    viewport clipped ``window_end``. Building the frame with the contract's explicit
    ``columns`` keeps ``window_start``/``window_end`` as the leading, always-visible
    identity columns. Every field and value is preserved (no truncation, rounding or
    removal); the remaining columns stay reachable through the grid's horizontal
    scroll. This is a supported Streamlit/pandas presentation adjustment, not a CSS
    override.
    """
    return pd.DataFrame(display_rows(rows), columns=list(TABLES[table]))


def render(config, demo, replay=False):
    if not demo:
        st.button("Refresh snapshot", key="refresh")
    step = None
    if replay:
        # The replay step is derived from elapsed time, so fragment reruns
        # (auto ticks, filter interactions, manual runs) never skip ahead.
        if st.button("Restart replay", key="replay_restart"):
            st.session_state["replay_started_at"] = replay_provider.now()
        started = st.session_state.setdefault("replay_started_at", replay_provider.now())
        step = replay_provider.advance_index(started, config.refresh_seconds)
    try:
        if replay:
            snapshot = replay_provider.replay_snapshot(config, step)
        elif demo:
            snapshot = synthetic_snapshot(config)
        else:
            snapshot = get_read_service(config).load()
    except DashboardUnavailable as exc:
        st.error(str(exc))
        st.info("No analytical values are displayed. Retry when a valid source is available.")
        return

    st.caption(f"Data source: {snapshot.source} | Redis status: {snapshot.redis_status}")
    if demo:
        st.caption(f"Synthetic source schema (contract shape): {snapshot.metadata['source_schema']} | "
                   f"Synthetic gold version: {snapshot.metadata['gold_version']} | "
                   f"Synthetic topic: {snapshot.metadata['source_topic']} | "
                   f"Contract status field: {snapshot.metadata['serving_status']}")
        if replay:
            st.caption(f"Replay step {step + 1} of {replay_provider.REPLAY_STEP_COUNT} — predefined snapshot "
                       f"served_at (scripted timestamp, not a live serve): {snapshot.metadata['served_at']}")
            st.caption("No Redis or PostgreSQL source was read; every value below comes from the scripted "
                       "synthetic replay sequence and changes only as that sequence advances.")
            if step == replay_provider.REPLAY_STEP_COUNT - 1:
                st.info(replay_provider.REPLAY_FINISHED_MESSAGE)
        else:
            st.caption(f"Synthetic served_at (fixed sample timestamp, not a live serve): {snapshot.metadata['served_at']}")
            st.caption("No Redis or PostgreSQL source was read; every value below comes from the committed synthetic sample.")
    else:
        st.caption(f"PostgreSQL source: {snapshot.metadata['source_schema']} | Gold version: {snapshot.metadata['gold_version']} | "
                   f"Source topic: {snapshot.metadata['source_topic']} | Status: {snapshot.metadata['serving_status']}")
        st.caption(f"PostgreSQL served_at: {snapshot.metadata['served_at']}")
        if snapshot.source == "Redis cache":
            st.info("Showing the cached source version. Current PostgreSQL freshness/availability has not been checked.")
        else:
            st.warning(f"Redis {snapshot.redis_status}; showing the authoritative PostgreSQL snapshot read for this refresh.")
    with st.expander("Snapshot provenance (synthetic replay)" if replay else
                     "Snapshot provenance (synthetic)" if demo else "Snapshot provenance"):
        if demo:
            # read_service's freshness wording describes live sources; synthetic provenance
            # states the real boundary instead of inheriting that claim.
            freshness = ("Predefined replay snapshots; no source was read at any time" if replay
                         else "Fixed synthetic sample; no source was read at any time")
            namespace = ("not used (replay mode constructs no Redis client)" if replay
                         else "not used (demo mode constructs no Redis client)")
            st.json({**snapshot.provenance, "freshness": freshness, "redis_namespace": namespace})
        else:
            st.json(snapshot.provenance)
    if snapshot.empty:
        st.info("Valid empty committed snapshot: zero revenue and product rows.")
        return

    currency_options = ["All currencies (separate views)", *snapshot.currencies]
    default_index = currency_options.index(config.default_currency) if config.default_currency in snapshot.currencies else 0
    if config.default_currency and default_index == 0:
        st.warning(f"Configured currency {config.default_currency} is absent; showing separate currency views.")
    # Phase 2C P1: the controls sit in a labelled three-column row so the
    # analytical content is visually separated from them. Widget keys, option
    # lists and the custom-range parsing are unchanged from M6, so every consumer
    # contract and render assertion still holds. st.sidebar cannot be used inside
    # an st.fragment, so the redesign stays in the main container.
    st.markdown("### Filters")
    currency_col, region_col, period_col = st.columns(3)
    currency = currency_col.selectbox("Currency", currency_options, index=default_index, key="currency")
    region = region_col.selectbox("Revenue region", [None, *sorted({r['region'] for r in snapshot.revenue_by_minute})],
                                  format_func=lambda value: "All regions" if value is None else value, key="region")
    period = period_col.selectbox("Time range", DEMO_PERIOD_OPTIONS if demo else REAL_PERIOD_OPTIONS, key="period")
    st.caption("Region filters revenue only. Product snapshots have no region dimension.")
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

    # Phase 2C P3: an at-a-glance summary derived from what the selection resolved
    # to, so the reader sees the exact row counts behind every figure below.
    st.caption(f"Active filters \u2014 currency: {currency}, region: {region or 'all regions'}, time range: {period}")
    st.caption(f"Matching rows \u2014 revenue: {len(filtered['revenue_by_minute'])}, "
               f"products: {len(filtered['product_rankings'])}")

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
                                 # Replay re-keys the widget per step so the default (latest committed window)
                                 # tracks the advancing snapshot instead of pinning a stale step-1 selection.
                                 key="kpi_" + code if not replay else f"kpi_{code}_step{step}")
            row = revenue[index]
            a, b, c = st.columns(3)
            a.metric(f"Net revenue — {code}", format_money(row["net_revenue"], code))
            b.metric(f"Orders — {code}", str(row["order_count"]))
            c.metric(f"Average order value — {code}", format_money(row["avg_order_value"], code))
            st.plotly_chart(revenue_chart(revenue, code), use_container_width=True)
            st.caption("Displayed amounts are rounded to two decimals \u2014 presentation only; stored values, "
                       "aggregations and filters keep the exact eight-place contract precision.")
            st.dataframe(display_frame(revenue, "revenue_by_minute"), use_container_width=True, hide_index=True)
        else:
            st.info(f"No revenue rows match these filters for {code}; this is not a zero-revenue assertion.")
        st.subheader(f"Products — {code}")
        products = [r for r in filtered["product_rankings"] if r["currency"] == code]
        if products:
            st.caption("Stored product/window rows; IDs, names and window boundaries remain distinct.")
            st.dataframe(display_frame(products, "product_rankings"), use_container_width=True, hide_index=True)
        else:
            st.info(f"No product rows match these filters for {code}.")
        # Phase 2C P3: currency-scoped insights recomputed from the very rows
        # rendered above, surfaced as text so no KPI/table/chart count changes.
        insights = currency_insights(revenue, products, code, region)
        if insights:
            with st.expander(f"Insights \u2014 {code}", expanded=False):
                st.caption("Derived from rows matching the current filters; currency-scoped, exact Decimal "
                           "math. Revenue honours the region filter; product lines are all-region.")
                for label, value in insights:
                    st.markdown(f"**{label}** \u2014 {value}")


def main():
    st.set_page_config(page_title="Order Intelligence Platform", page_icon="📦", layout="wide")
    st.title("📦 Order Intelligence Platform")
    mode = selected_mode()
    if mode == MODE_INVALID:
        # Fail before any real-source configuration or client construction.
        st.error(mode_error_message())
        return
    st.caption(MODE_SUBTITLES[mode])
    replay = mode == MODE_REPLAY
    demo = mode == MODE_DEMO or replay
    try:
        config = demo_config() if demo else DashboardConfig.from_env()
    except ValueError:
        st.error("Invalid dashboard configuration. Check source schema, Redis namespace, currency and refresh interval.")
        return
    if replay:
        st.warning(replay_provider.REPLAY_DISCLOSURE)
        st.caption(f"Scripted replay steps through {replay_provider.REPLAY_STEP_COUNT} predefined synthetic "
                   f"snapshots, advancing one snapshot every {config.refresh_seconds}s on the display refresh. "
                   "KPIs, chart, tables and insights always describe the active snapshot; nothing here queries "
                   "Kafka, Spark, PostgreSQL or Redis.")
    elif demo:
        st.warning(DEMO_DISCLOSURE)
        st.caption(f"Demo display refresh: {config.refresh_seconds}s. The synthetic sample is fixed; "
                   "refreshing re-renders identical values and never queries a source.")
    else:
        st.caption(f"Snapshot display refresh: {config.refresh_seconds}s. PostgreSQL is authoritative; Redis is a disposable cache.")
    # Streamlit fragment refresh replaces the previous blocking sleep/rerun loop.
    # In replay mode the same run_every reruns carry the scripted progression.
    st.fragment(run_every=config.refresh_seconds)(render)(config, demo, replay)


if __name__ == "__main__":
    main()
