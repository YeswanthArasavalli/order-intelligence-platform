"""Synthetic demo-mode tests: contract validation, disclosure and rendering.

Self-contained: no Kafka, Spark, MinIO, PostgreSQL, Redis, credentials, network
or ``.env`` file is touched. Real-source client construction is asserted absent.
"""

from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
import importlib
import json
from pathlib import Path
import re

import pytest
import redis
from streamlit.testing.v1 import AppTest

from dashboard import app as dashboard_app
from dashboard import demo_provider, read_service as service
from dashboard.demo_provider import (DEMO_DISCLOSURE, DEMO_MODE_ENV_VAR, DEMO_SOURCE_LABEL, MODE_DEMO,
                                     MODE_INVALID, MODE_REAL, PRODUCT_ROW_COUNT, REVENUE_ROW_COUNT)
from data_quality.currency_codes import VALID_CURRENCIES
from storage.postgres_redis_serving import KEYS, TABLES, _decode, _digest, _fields

APP = str(Path(__file__).resolve().parents[2] / "dashboard" / "app.py")
MONEY_COLUMNS = frozenset({"gross_revenue", "refund_amount", "cancellation_amount", "net_revenue",
                           "avg_order_value", "revenue"})
COUNT_COLUMNS = frozenset({"order_count", "failed_count", "refunded_event_count", "units_sold"})


def demo_config():
    return demo_provider.demo_config()


def payload():
    return demo_provider.synthetic_payload(demo_config())


def snapshot():
    return demo_provider.synthetic_snapshot(demo_config())


# ---------------------------------------------------------------------------
# flag selection (fail-safe)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    ("", MODE_REAL), ("synthetic", MODE_DEMO), ("Synthetic", MODE_INVALID), ("demo", MODE_INVALID),
    (" synthetic", MODE_INVALID), ("synthetic ", MODE_INVALID), ("0", MODE_INVALID), ("none", MODE_INVALID),
    ("real", MODE_INVALID)])
def test_mode_selection_is_fail_safe(monkeypatch, value, expected):
    monkeypatch.setenv(DEMO_MODE_ENV_VAR, value)
    assert demo_provider.selected_mode() == expected


def test_unset_flag_is_real_mode(monkeypatch):
    monkeypatch.delenv(DEMO_MODE_ENV_VAR, raising=False)
    assert demo_provider.selected_mode() == MODE_REAL


def test_mode_error_message_names_flag_and_value():
    message = demo_provider.mode_error_message("Synthetic")
    assert DEMO_MODE_ENV_VAR in message and "Synthetic" in message and "synthetic" in message


# ---------------------------------------------------------------------------
# dataset specification, schema, types and contract arithmetic
# ---------------------------------------------------------------------------

def test_documented_dataset_size_and_fixed_anchor():
    sample = payload()
    assert len(sample["revenue_by_minute"]) == REVENUE_ROW_COUNT == 12
    assert len(sample["product_rankings"]) == PRODUCT_ROW_COUNT == 12
    assert demo_provider.ANCHOR == datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)
    assert {row["window_start"] for row in sample["revenue_by_minute"]} == {
        (demo_provider.ANCHOR + timedelta(minutes=offset)).isoformat(timespec="microseconds")
        for offset in range(4)}
    assert {row["window_start"] for row in sample["product_rankings"]} == {
        demo_provider.ANCHOR.isoformat(timespec="microseconds")}


def test_rows_match_the_serving_contract_exactly():
    sample = payload()
    for table in TABLES:
        for row in sample[table]:
            assert set(row) == set(TABLES[table])
            for column in ("window_start", "window_end"):
                assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+00:00", row[column])
            for column in MONEY_COLUMNS & set(row):
                if row[column] is not None:
                    assert re.fullmatch(r"-?\d+\.\d{8}", row[column]), column
            for column in COUNT_COLUMNS & set(row):
                assert type(row[column]) is int and row[column] >= 0, column
            assert re.fullmatch(r"[A-Z]{3}", row["currency"])


def test_dimensions_are_valid_regions_and_iso_currencies():
    sample = payload()
    regions = {row["region"] for row in sample["revenue_by_minute"]}
    assert regions == {"north-india", "south-india", "international"} <= demo_provider.DEMO_REGIONS
    assert all(row["product_id"] and row["product_name"] and "\x00" not in row["product_name"]
               for row in sample["product_rankings"])
    currencies = {row["currency"] for row in (*sample["revenue_by_minute"], *sample["product_rankings"])}
    assert currencies == {"INR", "USD"} and currencies <= set(VALID_CURRENCIES)


def test_windows_are_exact_spans_and_identities_unique():
    sample = payload()
    for table, span in (("revenue_by_minute", timedelta(minutes=1)), ("product_rankings", timedelta(minutes=5))):
        rows = sample[table]
        for row in rows:
            start = datetime.fromisoformat(row["window_start"])
            assert datetime.fromisoformat(row["window_end"]) - start == span
        identities = [tuple(row[column] for column in KEYS[table]) for row in rows]
        assert len(set(identities)) == len(identities)


def test_decimal_counts_and_timestamps_are_typed_after_validation():
    got = snapshot()
    for row in got.revenue_by_minute:
        assert type(row["net_revenue"]) is Decimal and type(row["gross_revenue"]) is Decimal
        assert type(row["avg_order_value"]) in (Decimal, type(None))
        assert all(type(row[column]) is int for column in ("order_count", "failed_count", "refunded_event_count"))
        assert row["window_start"].tzinfo is timezone.utc and row["window_end"].tzinfo is timezone.utc
    for row in got.product_rankings:
        assert type(row["revenue"]) is Decimal and type(row["units_sold"]) is int


def test_ledger_and_average_rules_follow_the_gold_contract():
    for row in snapshot().revenue_by_minute:
        assert row["net_revenue"] == (row["gross_revenue"] - row["refund_amount"] - row["cancellation_amount"])
        if row["order_count"]:
            expected = (row["gross_revenue"] / Decimal(row["order_count"])).quantize(
                Decimal("0.00000001"), rounding=ROUND_HALF_UP)
            assert row["avg_order_value"] == expected
        else:
            assert row["avg_order_value"] is None


def test_representative_edge_cases_are_present():
    rows = snapshot().revenue_by_minute
    unpaid = [row for row in rows if row["order_count"] == 0]
    assert len(unpaid) == 1 and unpaid[0]["avg_order_value"] is None
    assert unpaid[0]["failed_count"] == 2 and unpaid[0]["net_revenue"] == Decimal("0")
    assert any(row["net_revenue"] < 0 for row in rows)
    assert any(row["refund_amount"] > 0 and row["cancellation_amount"] > 0 for row in rows)
    # Eight-decimal ROUND_HALF_UP boundary: 2.00000001 / 2 -> 1.00000001.
    boundary = next(row for row in rows if row["gross_revenue"] == Decimal("2.00000001"))
    assert boundary["avg_order_value"] == Decimal("1.00000001")


def test_product_totals_tie_to_revenue_totals_per_currency():
    got = snapshot()
    for currency in ("INR", "USD"):
        orders = sum(row["order_count"] for row in got.revenue_by_minute if row["currency"] == currency)
        units = sum(row["units_sold"] for row in got.product_rankings if row["currency"] == currency)
        gross = sum((row["gross_revenue"] for row in got.revenue_by_minute if row["currency"] == currency), Decimal(0))
        revenue = sum((row["revenue"] for row in got.product_rankings if row["currency"] == currency), Decimal(0))
        assert orders == units and gross == revenue


def test_dataset_is_deterministic_across_calls_and_reload():
    first = snapshot()
    reloaded = importlib.reload(demo_provider)
    for other in (snapshot(), reloaded.synthetic_snapshot(demo_config())):
        assert other.metadata == first.metadata
        assert other.revenue_by_minute == first.revenue_by_minute
        assert other.product_rankings == first.product_rankings
    assert json.dumps(payload()["metadata"], sort_keys=True) == json.dumps(
        importlib.reload(demo_provider).synthetic_payload(demo_config())["metadata"], sort_keys=True)


# ---------------------------------------------------------------------------
# serving-contract round trip through the real M5 encoder/decoder
# ---------------------------------------------------------------------------

def test_wire_round_trip_through_the_real_m5_serializer():
    sample = payload()
    digest = _digest({table: list(sample[table]) for table in TABLES})
    assert sample["metadata"]["snapshot_digest"] == digest
    fields = _fields(sample)
    assert list(fields)[0] == "@metadata" and len(fields) == 1 + REVENUE_ROW_COUNT + PRODUCT_ROW_COUNT
    assert all(key.startswith(("revenue:", "product:")) for key in fields if key != "@metadata")
    decoded = _decode(fields)
    assert decoded == sample
    assert _digest({table: list(decoded[table]) for table in TABLES}) == digest


def test_metadata_satisfies_the_consumer_validation():
    config, metadata = demo_config(), payload()["metadata"]
    assert metadata["serving_status"] == "SERVED" and metadata["cache_contract_version"] == "1.0"
    assert metadata["gold_contract_version"] == "1.0" and type(metadata["gold_version"]) is int
    assert metadata["source_schema"] == config.schema and metadata["source_database"] == config.database
    assert metadata["revenue_rows"] == REVENUE_ROW_COUNT and metadata["product_rows"] == PRODUCT_ROW_COUNT
    for name in ("snapshot_digest", "manifest_digest", "snapshot_identity"):
        assert re.fullmatch(r"[0-9a-f]{64}", metadata[name])
    assert datetime.fromisoformat(metadata["served_at"]).tzinfo is not None
    for name in ("source_topic", "source_root", "manifest_path"):
        assert "synthetic" in metadata[name], name


def test_snapshot_is_labelled_synthetic_and_populated():
    got = snapshot()
    assert got.source == DEMO_SOURCE_LABEL and not got.empty
    assert got.redis_status == "not used (demo)"
    assert got.currencies == ["INR", "USD"]
    assert got.provenance["data_source"] == DEMO_SOURCE_LABEL


def test_filters_keep_the_whole_sample_in_the_default_view():
    got = snapshot()
    assert len(service.filter_rows(got)["revenue_by_minute"]) == REVENUE_ROW_COUNT
    assert len(service.filter_rows(got)["product_rankings"]) == PRODUCT_ROW_COUNT
    assert len(service.filter_rows(got, currency="INR")["product_rankings"]) == 7
    assert len(service.filter_rows(got, region="north-india")["revenue_by_minute"]) == 4


def test_provider_has_no_real_source_or_write_entrypoints():
    exposed = set(dir(demo_provider))
    assert not exposed & {"serve_postgres_snapshot", "delete_cache", "get_redis_client",
                          "get_postgres_engine", "read_postgres_snapshot", "read_active_snapshot"}


def test_launcher_targets_the_dashboard_with_explicit_selection(monkeypatch):
    from dashboard import demo as demo_launcher
    monkeypatch.delenv("PYTHONPATH", raising=False)
    environment = demo_launcher.launch_environment()
    assert environment[DEMO_MODE_ENV_VAR] == "synthetic"
    assert environment["PYTHONPATH"] == str(demo_launcher.ROOT)
    command = demo_launcher.streamlit_command()
    assert command[:3] == [demo_launcher.sys.executable, "-m", "streamlit"]
    assert Path(command[-1]) == demo_launcher.APP and demo_launcher.APP.name == "app.py"


# ---------------------------------------------------------------------------
# rendering through the unchanged Streamlit application
# ---------------------------------------------------------------------------

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


def test_real_and_demo_period_option_lists_are_distinct_and_stable():
    assert dashboard_app.REAL_PERIOD_OPTIONS == ["All snapshot windows", "Last 30 minutes (UTC now)",
                                                 "Custom UTC range"]
    assert dashboard_app.DEMO_PERIOD_OPTIONS == ["All synthetic sample windows", "Custom UTC range"]


def run_demo_app(isolated):
    isolated.setenv(DEMO_MODE_ENV_VAR, "synthetic")
    app = AppTest.from_file(APP).run(timeout=30)
    assert not app.exception, [str(exception.value) for exception in app.exception]
    return app


def test_demo_banner_and_synthetic_source_are_always_disclosed(isolated):
    app = run_demo_app(isolated)
    assert DEMO_DISCLOSURE in {warning.value for warning in app.warning}
    assert any(f"Data source: {DEMO_SOURCE_LABEL}" in caption.value for caption in app.caption)
    assert any("no redis or postgresql source was read" in caption.value.lower() for caption in app.caption)
    assert not [button for button in app.button if button.label == "Refresh snapshot"]
    assert app.selectbox(key="period").options == dashboard_app.DEMO_PERIOD_OPTIONS
    assert app.selectbox(key="period").value == "All synthetic sample windows"
    assert not any("Last 30 minutes" in str(option) for option in app.selectbox(key="period").options)


def test_demo_initial_view_is_populated_with_exact_values(isolated):
    app = run_demo_app(isolated)
    assert not app.error and not app.info
    assert len(app.metric) == 6 and len(app.dataframe) == 4
    assert [metric.value for metric in app.metric] == [
        "INR 12,830.50000005", "3", "INR 5,110.16666668",
        "USD 18,890.00999999", "3", "USD 6,710.00333333"]
    assert all(not frame.value.empty for frame in app.dataframe)
    # INR revenue, INR products, USD revenue, USD products for the full sample.
    assert [frame.value.shape[0] for frame in app.dataframe] == [8, 7, 4, 5]
    assert all(isinstance(value, str) for value in app.dataframe[1].value["revenue"])


def test_demo_charts_and_filters_render_sample_views(isolated):
    app = run_demo_app(isolated)
    assert len(app.get("plotly_chart")) == 2
    app.selectbox(key="currency").select("INR").run()
    assert not app.exception and len(app.metric) == 3 and len(app.dataframe) == 2
    assert set(app.dataframe[0].value["currency"]) == {"INR"}
    app.selectbox(key="region").select("north-india").run()
    assert not app.exception and set(app.dataframe[0].value["region"]) == {"north-india"}
    assert len(app.dataframe[1].value) == 7  # products have no region dimension


def test_demo_custom_range_covers_the_fixed_sample(isolated):
    app = run_demo_app(isolated)
    app.selectbox(key="period").select("Custom UTC range").run()
    assert not app.exception
    # Streamlit seeds the bounds from the stored sample, not from the clock.
    assert app.text_input(key="start").value == "2026-10-05T12:00:00+00:00"
    assert app.text_input(key="end").value == "2026-10-05T12:05:00+00:00"
    assert len(app.metric) == 6 and len(app.dataframe) == 4


def test_invalid_flag_stops_before_any_real_source(isolated):
    isolated.setenv(DEMO_MODE_ENV_VAR, "Synthetic")
    app = AppTest.from_file(APP).run(timeout=30)
    assert not app.exception
    assert any(DEMO_MODE_ENV_VAR in error.value and "Synthetic" in error.value for error in app.error)
    assert not app.metric and not app.dataframe and not app.selectbox
    assert DEMO_DISCLOSURE not in {warning.value for warning in app.warning}


@pytest.mark.parametrize("flag", [None, ""])
def test_real_mode_keeps_source_selection_and_failure_behavior(monkeypatch, flag):
    """Unset/empty flag: the real Redis then PostgreSQL path, with an explicit failure.

    The Redis client constructor refuses so the outcome never depends on ambient
    infrastructure, and no PostgreSQL password exists in a demo environment. Real
    mode must therefore fail loudly instead of silently falling back to synthetic data.
    """
    attempts = []

    def refused(*args, **kwargs):
        attempts.append("redis")
        raise redis.ConnectionError("no Redis server in this environment")

    if flag is None:
        monkeypatch.delenv(DEMO_MODE_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(DEMO_MODE_ENV_VAR, flag)
    for key in ("DASHBOARD_SOURCE_TOPIC", "DASHBOARD_DEFAULT_CURRENCY", "DASHBOARD_REFRESH_SECONDS",
                "POSTGRES_PASSWORD", "POSTGRES_GOLD_SCHEMA", "REDIS_GOLD_NAMESPACE"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(service.redis, "Redis", refused)
    app = AppTest.from_file(APP).run(timeout=30)
    assert not app.exception
    assert attempts == ["redis"]
    assert any("Dashboard data unavailable: Redis unavailable; authoritative PostgreSQL unavailable" in error.value
               for error in app.error)
    assert DEMO_DISCLOSURE not in {warning.value for warning in app.warning}
    assert DEMO_SOURCE_LABEL not in {caption.value for caption in app.caption}
    assert any(button.label == "Refresh snapshot" for button in app.button)
    assert not app.selectbox  # the real path stops before filters when no source is available
    assert not app.metric and not app.dataframe
