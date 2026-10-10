"""Phase 2C tests: genuine Streamlit filter interaction and data-derived insights.

Self-contained like the M6 demo suite: no Kafka, Spark, MinIO, PostgreSQL, Redis,
credentials, network or ``.env`` is touched, and real-source client construction is
asserted absent. The Phase 2B suite (``test_dashboard_demo.py``) stays untouched;
this file adds coverage for the two Phase 2C additions:

* ``currency_insights`` — a pure function that recomputes currency-scoped insight
  lines from the exact rows the tables render, cross-checked against independent
  Decimal arithmetic in these tests rather than asserted from hand.
* The redesigned render path — filter widgets drive the at-a-glance summary and the
  insight text, so a change of selection is observable end to end.
"""

from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from dashboard import app as dashboard_app
from dashboard import demo_provider, read_service as service
from dashboard.demo_provider import DEMO_MODE_ENV_VAR

APP = str(Path(__file__).resolve().parents[2] / "dashboard" / "app.py")

# The exact separators the app renders with; kept as explicit code points so these
# expectations never depend on this source file's on-disk encoding.
EM = "\u2014"   # em dash, the "label — value" joiner
EN = "\u2013"   # en dash, the window span separator
ARROW = "\u2192"  # right arrow, the first->last trend wording


def demo_snapshot():
    return demo_provider.synthetic_snapshot(demo_provider.demo_config())


def rows_for(snapshot, code, region=None):
    """Per-currency filtered rows exactly as the render loop builds them."""
    filtered = service.filter_rows(snapshot, currency=code, region=region)
    return ([r for r in filtered["revenue_by_minute"] if r["currency"] == code],
            [r for r in filtered["product_rankings"] if r["currency"] == code])


@pytest.fixture()
def isolated(monkeypatch):
    """Clean dashboard environment; any real client construction or use fails loudly."""

    def forbidden(*args, **kwargs):
        raise AssertionError("demo mode must not construct or use real source clients")

    monkeypatch.setattr(service.DashboardReadService, "__init__", forbidden)
    monkeypatch.setattr(service.DashboardReadService, "load", forbidden)
    monkeypatch.setattr(service.redis, "Redis", forbidden)
    monkeypatch.setattr("storage.postgres_redis_serving.create_engine", forbidden)
    for key in (DEMO_MODE_ENV_VAR, "DASHBOARD_SOURCE_TOPIC", "DASHBOARD_DEFAULT_CURRENCY",
                "DASHBOARD_REFRESH_SECONDS", "POSTGRES_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


def run_demo_app(isolated):
    isolated.setenv(DEMO_MODE_ENV_VAR, "synthetic")
    app = AppTest.from_file(APP).run(timeout=30)
    assert not app.exception, [str(exception.value) for exception in app.exception]
    return app


def insight_text(app):
    """All markdown the app emitted, joined for substring inspection."""
    return "\n".join(markdown.value for markdown in app.markdown)


def caption_text(app):
    return [caption.value for caption in app.caption]


# ---------------------------------------------------------------------------
# currency_insights: pure, data-derived, currency-scoped arithmetic
# ---------------------------------------------------------------------------

def test_empty_selection_yields_no_insight_lines():
    assert dashboard_app.currency_insights([], [], "INR") == []


def test_leading_region_is_summed_from_the_net_revenue_rows():
    revenue, products = rows_for(demo_snapshot(), "INR")
    # Independent computation: group net_revenue by region over the same rows.
    expected = {}
    for row in revenue:
        expected[row["region"]] = expected.get(row["region"], Decimal(0)) + row["net_revenue"]
    # Deterministic tie-break: highest net revenue, then ascending region name.
    lead_region = min(expected.items(), key=lambda item: (-item[1], item[0]))[0]
    line = dashboard_app.currency_insights(revenue, products, "INR")[0]
    assert line == (f"Leading region by net revenue ({len(expected)} in view, all regions)",
                    f"{lead_region} {EM} {service.format_money(expected[lead_region], 'INR')}")


def test_paid_event_totals_match_the_stored_counts():
    revenue, products = rows_for(demo_snapshot(), "INR")
    lines = dict(dashboard_app.currency_insights(revenue, products, "INR"))
    paid_key = next(key for key in lines if key.startswith("Paid events in view"))
    event_key = next(key for key in lines if key.startswith("Paid product events in view"))
    assert lines[paid_key] == str(sum(row["order_count"] for row in revenue))
    assert lines[event_key] == str(sum(row["units_sold"] for row in products))


def test_top_product_is_the_maximum_revenue_row_within_the_currency():
    revenue, products = rows_for(demo_snapshot(), "INR")
    lines = dict(dashboard_app.currency_insights(revenue, products, "INR"))
    top = max(products, key=lambda row: (row["revenue"], row["product_id"]))
    top_key = next(key for key in lines if key.startswith("Top product by revenue"))
    assert lines[top_key] == (
        f"{top['product_name']} ({top['product_id']}) {EM} "
        f"{service.format_money(top['revenue'], 'INR')}, {top['units_sold']} paid events")


def test_deduction_rate_follows_the_ledger_arithmetic():
    revenue, products = rows_for(demo_snapshot(), "INR")
    gross = sum((row["gross_revenue"] for row in revenue), Decimal(0))
    deducted = sum((row["refund_amount"] + row["cancellation_amount"] for row in revenue), Decimal(0))
    expected_rate = (deducted / gross * 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    lines = dict(dashboard_app.currency_insights(revenue, products, "INR"))
    rate_key = next(key for key in lines if key.startswith("Combined refund + cancellation"))
    # The percentage arithmetic is unchanged; only the labelling/clarification differs.
    assert lines[rate_key].startswith(f"{expected_rate}% of gross ({service.format_money(deducted, 'INR')})")


def test_net_revenue_trend_direction_is_derived_from_first_and_last_window():
    revenue, products = rows_for(demo_snapshot(), "INR")
    windows = sorted({row["window_start"] for row in revenue})
    first = sum((r["net_revenue"] for r in revenue if r["window_start"] == windows[0]), Decimal(0))
    last = sum((r["net_revenue"] for r in revenue if r["window_start"] == windows[-1]), Decimal(0))
    delta = last - first
    trend = "rising" if delta > 0 else "falling" if delta < 0 else "flat"
    lines = [line for line in dashboard_app.currency_insights(revenue, products, "INR")
             if line[0].startswith("Net revenue ")]
    assert len(lines) == 1
    label, value = lines[0]
    assert label == (f"Net revenue {trend} across {len(windows)} windows "
                     f"({windows[0].strftime('%H:%M')}{EN}{windows[-1].strftime('%H:%M')} UTC)")
    assert value == f"first{ARROW}last change {service.format_money(delta, 'INR')}"


def test_single_window_selection_drops_the_trend_line():
    snapshot = demo_snapshot()
    first_minute = min(row["window_start"] for row in snapshot.revenue_by_minute)
    revenue = [r for r in snapshot.revenue_by_minute
               if r["currency"] == "INR" and r["window_start"] == first_minute]
    labels = [label for label, _ in dashboard_app.currency_insights(revenue, [], "INR")]
    assert not any(label.startswith("Net revenue ") for label in labels)


def test_insights_never_mix_currencies():
    snapshot = demo_snapshot()
    for code, other in (("INR", "USD"), ("USD", "INR")):
        revenue, products = rows_for(snapshot, code)
        rendered = " ".join(value for _, value in dashboard_app.currency_insights(revenue, products, code))
        assert rendered.count(code) >= 1 and other not in rendered


def test_paid_event_labels_replace_orders_and_units():
    revenue, products = rows_for(demo_snapshot(), "INR")
    labels = [label for label, _ in dashboard_app.currency_insights(revenue, products, "INR")]
    assert any(label.startswith("Paid events in view") for label in labels)
    assert any(label.startswith("Paid product events in view") for label in labels)
    # The contract calls these PAID-EVENT counts, so no order/units misnomer remains.
    assert not any("Paid orders" in label for label in labels)
    assert not any("Units sold" in label for label in labels)


def test_region_scope_separates_revenue_and_product_insights():
    snapshot = demo_snapshot()
    revenue, products = rows_for(snapshot, "INR", region="north-india")
    labels = dict(dashboard_app.currency_insights(revenue, products, "INR", region="north-india"))
    paid_key = next(key for key in labels if key.startswith("Paid events in view"))
    product_key = next(key for key in labels if key.startswith("Paid product events in view"))
    # Revenue honours the selected region; product lines stay explicitly all-region.
    assert "region north-india" in paid_key and "all regions" not in paid_key
    assert "all regions" in product_key


def test_leading_region_ties_break_on_region_name():
    base = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)

    def row(region, net):
        return {"region": region, "net_revenue": net, "order_count": 1, "window_start": base,
                "gross_revenue": net, "refund_amount": Decimal(0), "cancellation_amount": Decimal(0)}

    revenue = [row("zeta-region", Decimal("100")), row("alpha-region", Decimal("100"))]
    lead = dashboard_app.currency_insights(revenue, [], "INR")[0]
    # Equal net revenue must resolve to the alphabetically first region, deterministically.
    assert lead[1].startswith(f"alpha-region {EM}")


def test_combined_reversal_metric_documents_the_100_percent_caveat():
    revenue, products = rows_for(demo_snapshot(), "INR")
    labels = dict(dashboard_app.currency_insights(revenue, products, "INR"))
    rate_key = next(key for key in labels if key.startswith("Combined refund + cancellation"))
    assert "additive ledger" in rate_key
    assert "exceed 100%" in labels[rate_key]


# ---------------------------------------------------------------------------
# redesigned render path: genuine filter interaction drives summary + insights
# ---------------------------------------------------------------------------

def test_full_view_at_a_glance_counts_match_the_sample(isolated):
    app = run_demo_app(isolated)
    assert any(f"Matching rows {EM} revenue: 12, products: 12" in value for value in caption_text(app))


def test_region_selection_updates_summary_and_leading_region(isolated):
    app = run_demo_app(isolated)
    app.selectbox(key="currency").select("INR").run()
    app.selectbox(key="region").select("north-india").run()
    assert not app.exception
    # Revenue is region-filtered (4 north-india windows); products keep their 7 rows.
    assert any(f"Matching rows {EM} revenue: 4, products: 7" in value for value in caption_text(app))
    text = insight_text(app)
    assert "Leading region by net revenue (1 in view, region north-india)" in text
    assert "north-india" in text and "26,750.00000001" in text


def test_region_switch_changes_the_derived_leader(isolated):
    app = run_demo_app(isolated)
    app.selectbox(key="currency").select("INR").run()
    app.selectbox(key="region").select("south-india").run()
    assert "south-india" in insight_text(app) and "38,981.75000000" in insight_text(app)
    app.selectbox(key="region").select("north-india").run()
    text = insight_text(app)
    assert "north-india" in text and "26,750.00000001" in text and "38,981.75000000" not in text


def test_currency_selection_scopes_the_insight_currency(isolated):
    app = run_demo_app(isolated)
    app.selectbox(key="currency").select("INR").run()
    assert "INR 38,981.75000000" in insight_text(app)
    app.selectbox(key="currency").select("USD").run()
    text = insight_text(app)
    assert "USD 33,129.99999999" in text and "INR 38,981.75000000" not in text


def test_out_of_range_window_clears_values_summary_and_insights(isolated):
    app = run_demo_app(isolated)
    app.selectbox(key="period").select("Custom UTC range").run()
    app.text_input(key="start").set_value("2020-01-01T00:00:00+00:00")
    app.text_input(key="end").set_value("2020-01-02T00:00:00+00:00").run()
    assert not app.exception and not app.error
    assert not app.metric and not app.dataframe
    assert any(f"Matching rows {EM} revenue: 0, products: 0" in value for value in caption_text(app))
    assert "Leading region by net revenue" not in insight_text(app)


def test_redesign_preserves_the_frozen_metric_and_table_counts(isolated):
    app = run_demo_app(isolated)
    # Phase 2B invariants must survive the additive redesign unchanged.
    assert len(app.metric) == 6 and len(app.dataframe) == 4 and len(app.get("plotly_chart")) == 2
    assert {header.value for header in app.subheader} == {
        "Revenue — INR", "Revenue — USD", "Products — INR", "Products — USD"}


def test_partial_time_range_changes_rows_and_leader(isolated):
    app = run_demo_app(isolated)
    app.selectbox(key="currency").select("INR").run()
    app.selectbox(key="period").select("Custom UTC range").run()
    # Keep only the first two one-minute windows: 4 INR revenue rows (2 windows x 2 regions).
    app.text_input(key="start").set_value("2026-10-05T12:00:00+00:00")
    app.text_input(key="end").set_value("2026-10-05T12:02:00+00:00").run()
    assert not app.exception and not app.error
    assert any(f"Matching rows {EM} revenue: 4, products: 7" in value for value in caption_text(app))
    # Leader over the two selected windows (north 16,248 vs south 26,500.25) is south-india.
    assert "26,500.24999995" in insight_text(app)


def test_currency_region_and_time_filters_apply_together(isolated):
    app = run_demo_app(isolated)
    app.selectbox(key="currency").select("INR").run()
    app.selectbox(key="period").select("Custom UTC range").run()
    app.text_input(key="start").set_value("2026-10-05T12:00:00+00:00")
    app.text_input(key="end").set_value("2026-10-05T12:02:00+00:00")
    app.selectbox(key="region").select("south-india").run()
    assert not app.exception and not app.error
    # Revenue is currency+region+time filtered (2 rows); products keep currency+time only (7).
    assert any(f"Matching rows {EM} revenue: 2, products: 7" in value for value in caption_text(app))
    text = insight_text(app)
    assert "region south-india" in text and "26,500.24999995" in text
