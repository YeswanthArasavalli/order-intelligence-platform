"""M6 read-contract and actual Streamlit-render tests with independent fixtures."""

import base64
import copy
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path

import pytest
import redis
import streamlit as st
from sqlalchemy.exc import OperationalError
from streamlit.testing.v1 import AppTest

from dashboard import app as dashboard_app
from dashboard import read_service as service
from dashboard.demo_provider import (DEMO_DISCLOSURE, DEMO_MODE_ENV_VAR, DEMO_MODE_VALUE, MODE_DEMO, MODE_REAL,
                                     selected_mode)
from scripts.verify_milestone5 import canonical

APP = str(Path(__file__).resolve().parents[2] / "dashboard" / "app.py")

# ---------------------------------------------------------------------------
# P1 test isolation: the render path takes its mode and its whole configuration
# from the environment. ``dashboard/app.py`` branches on ``selected_mode()`` and
# real mode builds ``DashboardConfig.from_env()``, so any value exported by the
# launching shell decides what a render test sees. The boundary below removes
# that dependency instead of hoping the environment is clean.
# ---------------------------------------------------------------------------

ISOLATED_ENV_KEYS = (DEMO_MODE_ENV_VAR, "POSTGRES_GOLD_SCHEMA", "POSTGRES_DB", "REDIS_GOLD_NAMESPACE",
                     "DASHBOARD_SOURCE_TOPIC", "DASHBOARD_DEFAULT_CURRENCY", "DASHBOARD_REFRESH_SECONDS")


def clear_streamlit_caches() -> None:
    """Empty the process-global ``st.cache_resource`` store the app fills.

    ``AppTest`` execs ``dashboard/app.py`` under its own module identity, so the
    cached entry belongs to that run, not to the imported module object: calling
    ``dashboard_app.get_read_service.clear()`` from the test process leaves the
    entry in place (verified against streamlit 1.40.1, where a following session
    still reused the cached service). The global API is the mechanism that works.
    Production code is untouched: the cache itself stays exactly where it is.
    """
    st.cache_resource.clear()


@contextmanager
def real_mode_boundary(monkeypatch):
    """Force real mode with an empty cache, and keep the cache empty on the way out.

    A context manager rather than only a fixture body, so the finalizer also runs
    when the enclosed test raises: a failed render must not hand its cached
    ``DashboardReadService`` to the next test. ``monkeypatch.delenv`` restores the
    environment itself through pytest's own teardown, including on failure.
    """
    for key in ISOLATED_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    clear_streamlit_caches()
    try:
        yield
    finally:
        clear_streamlit_caches()


@pytest.fixture(autouse=True)
def isolate_dashboard_environment(monkeypatch):
    """Start every test here with ``OIP_DEMO_MODE`` unset and no cached app state."""
    with real_mode_boundary(monkeypatch):
        yield


def fixture_snapshot():
    base = dict(window_start="2026-10-04T00:00:00.123456+00:00", window_end="2026-10-04T00:01:00.123456+00:00",
                region="north:雪", currency="INR", order_count=1, failed_count=1, refunded_event_count=1,
                gross_revenue="100.00000000", refund_amount="30.00000001", cancellation_amount="100.00000000",
                net_revenue="-30.00000001", avg_order_value=None)
    product = dict(window_start="2026-10-04T00:00:00.123456+00:00", window_end="2026-10-04T00:05:00.123456+00:00",
                   product_id="same:id", product_name="Name", currency="INR", units_sold=1,
                   revenue="999999999999999999999999999999.99999999")
    tables = {"revenue_by_minute": [base, {**base, "currency": "USD", "net_revenue": "20.00000000"},
                                     {**base, "region": "south", "avg_order_value": "100.00000000"}],
              "product_rankings": [product, {**product, "currency": "USD"}, {**product, "product_name": "Other"},
                                   {**product, "window_end": "2026-10-04T00:10:00.123456+00:00"}]}
    digest, _ = canonical(tables, cache=True)
    meta = dict(cache_contract_version="1.0", gold_contract_version="1.0", gold_version=3,
                source_database="order_intelligence", source_schema="gold_serving", source_topic="test-topic",
                source_root="s3a://gold/test-topic", manifest_path="provenance-only", manifest_digest="a" * 64,
                snapshot_identity="b" * 64, snapshot_digest=digest, revenue_rows=3, product_rows=4,
                served_at="2026-10-04T01:00:00.000000+00:00", serving_status="SERVED")
    return {"metadata": meta, **tables}


def hash_fixture(snapshot):
    """Independent test encoder, no M5 private serializer or field-builder reuse."""
    result = {"@metadata": json.dumps(snapshot["metadata"])}
    for table, family, dims in (("revenue_by_minute", "revenue", ["window_start", "window_end", "region"]),
                                ("product_rankings", "product", ["window_start", "window_end", "product_id", "product_name"])):
        for row in snapshot[table]:
            key = family + ":" + ":".join(base64.urlsafe_b64encode(row[c].encode()).decode().rstrip("=") or "_"
                                           for c in dims) + ":" + row["currency"]
            result[key] = json.dumps(row)
    return result


class FakeRedis:
    def __init__(self, fields):
        self.fields, self.calls = fields, []

    def hgetall(self, key):
        self.calls.append(key)
        if isinstance(self.fields, Exception):
            raise self.fields
        return copy.deepcopy(self.fields)

    def close(self):
        pass


@pytest.fixture()
def setup(monkeypatch):
    snapshot = fixture_snapshot()
    client = FakeRedis(hash_fixture(snapshot))
    calls = []

    def pg_read(engine, schema):
        calls.append(schema)
        return copy.deepcopy(snapshot)

    monkeypatch.setattr(service, "read_postgres_snapshot", pg_read)
    reader = service.DashboardReadService(service.DashboardConfig(), redis_factory=lambda: client, postgres_factory=lambda: object())
    return snapshot, client, calls, reader


def test_valid_redis_preferred_and_no_legacy_keys(setup):
    _, client, calls, reader = setup
    got = reader.load()
    assert got.source == "Redis cache" and got.redis_status == "valid"
    assert calls == [] and client.calls == ["oip:m5:gold-serving:current"]


@pytest.mark.parametrize("state,status", [(redis.ConnectionError("offline"), "unavailable"), ({}, "missing"),
                                          ({"@metadata": "broken"}, "invalid"), ({"@metadata": "[]"}, "invalid")])
def test_redis_failure_fallback(setup, state, status):
    _, client, calls, reader = setup
    client.fields = state
    got = reader.load()
    assert got.source == "PostgreSQL fallback" and got.redis_status == status
    assert calls == ["gold_serving"]


@pytest.mark.parametrize("field,value", [("snapshot_digest", "0" * 64), ("revenue_rows", 0),
                                        ("gold_version", None), ("gold_version", True),
                                        ("source_schema", "foreign"), ("source_database", "foreign"),
                                        ("source_topic", ""), ("served_at", "invalid"), ("manifest_digest", "invalid")])
def test_invalid_metadata_rejected(setup, field, value):
    source, client, _, reader = setup
    invalid = copy.deepcopy(source)
    invalid["metadata"][field] = value
    client.fields = hash_fixture(invalid)
    assert reader.load().redis_status == "invalid"


@pytest.mark.parametrize("field,value", [("net_revenue", 1.1), ("net_revenue", "NaN"), ("currency", "usd"),
                                        ("window_end", "not-time"), ("order_count", -1)])
def test_invalid_redis_row_falls_back(setup, field, value):
    _, client, _, reader = setup
    key = next(k for k in client.fields if k.startswith("revenue:"))
    row = json.loads(client.fields[key])
    row[field] = value
    client.fields[key] = json.dumps(row)
    assert reader.load().source == "PostgreSQL fallback"


def test_postgres_unavailable_valid_cache_works(setup, monkeypatch):
    _, _, calls, reader = setup
    monkeypatch.setattr(service, "read_postgres_snapshot", lambda *a: pytest.fail("Valid Redis must not query PG"))
    assert "freshness not checked" in reader.load().provenance["freshness"]
    assert not calls


@pytest.mark.parametrize("cache", [redis.ConnectionError("offline"), {"@metadata": "invalid"}, {}])
def test_both_unavailable_explicit_failure(setup, monkeypatch, cache):
    _, client, _, reader = setup
    client.fields = cache

    def unavailable(*args):
        raise OperationalError("secret DSN must not be exposed", {}, Exception("offline"))

    monkeypatch.setattr(service, "read_postgres_snapshot", unavailable)
    with pytest.raises(service.DashboardUnavailable, match="data unavailable") as exc:
        reader.load()
    assert "secret" not in str(exc.value)


def test_all_currencies_full_product_identity_exact_values(setup):
    source, _, _, reader = setup
    got = reader.load()
    assert got.currencies == ["INR", "USD"]
    assert len(got.product_rankings) == 4
    assert got.revenue_by_minute[0]["net_revenue"] == Decimal("-30.00000001")
    assert got.revenue_by_minute[0]["avg_order_value"] is None
    assert got.product_rankings[0]["revenue"] == Decimal("999999999999999999999999999999.99999999")
    tables = {"revenue_by_minute": got.revenue_by_minute, "product_rankings": got.product_rankings}
    assert canonical(tables)[0] == source["metadata"]["snapshot_digest"]
    # Display is the shared two-decimal boundary; the stored Decimals above are untouched.
    assert service.format_money(got.revenue_by_minute[0]["net_revenue"], "INR") == "INR -30.00"
    assert service.display_rows(got.product_rankings)[0]["revenue"] == "1,000,000,000,000,000,000,000,000,000,000.00"
    assert got.revenue_by_minute[0]["net_revenue"] == Decimal("-30.00000001")
    assert got.product_rankings[0]["revenue"] == Decimal("999999999999999999999999999999.99999999")


def test_filters_are_dimension_preserving(setup):
    _, _, _, reader = setup
    got = reader.load()
    assert len(service.filter_rows(got)["revenue_by_minute"]) == 3
    assert len(service.filter_rows(got, currency="USD")["revenue_by_minute"]) == 1
    assert len(service.filter_rows(got, currency="INR")["product_rankings"]) == 3
    region = service.filter_rows(got, region="south")
    assert len(region["revenue_by_minute"]) == 1 and len(region["product_rankings"]) == 4
    stamp = got.revenue_by_minute[0]["window_start"]
    assert len(service.filter_rows(got, start=stamp)["revenue_by_minute"]) == 3
    assert not service.filter_rows(got, end=stamp)["revenue_by_minute"]
    assert not service.filter_rows(got, region="nonexistent")["revenue_by_minute"]
    with pytest.raises(ValueError):
        service.filter_rows(got, start=stamp, end=stamp)
    with pytest.raises(ValueError):
        service.filter_rows(got, start=datetime(2026, 1, 1))


def test_valid_empty_distinct_from_missing(setup):
    source, client, calls, reader = setup
    source.update(revenue_by_minute=[], product_rankings=[])
    source["metadata"].update(revenue_rows=0, product_rows=0,
                              snapshot_digest=hashlib.sha256(b'{"product_rankings":[],"revenue_by_minute":[]}').hexdigest())
    client.fields = hash_fixture(source)
    assert reader.load().empty and not calls


def test_provenance(setup):
    source, _, _, reader = setup
    got = reader.load()
    assert all(got.provenance[k] == source["metadata"][k] for k in ("source_schema", "gold_version", "snapshot_digest",
                                                                 "served_at", "source_topic"))


def test_expected_topic_mismatch_rejected(setup):
    _, _, _, reader = setup
    reader.config = service.DashboardConfig(expected_topic="different-topic")
    with pytest.raises(service.DashboardUnavailable):
        reader.load()


def test_streamlit_region_custom_time_and_explicit_default(setup, monkeypatch):
    _, _, _, reader = setup
    snapshot = reader.load()
    monkeypatch.setattr(service.DashboardReadService, "load", lambda self: snapshot)
    monkeypatch.setenv("POSTGRES_GOLD_SCHEMA", "gold_serving")
    monkeypatch.setenv("DASHBOARD_DEFAULT_CURRENCY", "INR")
    app = AppTest.from_file(APP).run(timeout=20)
    assert app.selectbox(key="currency").value == "INR"
    app.selectbox(key="region").select("south").run()
    assert set(app.dataframe[0].value["region"]) == {"south"}
    assert len(app.dataframe[1].value) == 3  # Product source has no region dimension.
    app.selectbox(key="period").select("Custom UTC range").run()
    app.text_input(key="start").set_value("2026-10-04T00:00:00+00:00")
    app.text_input(key="end").set_value("2026-10-04T00:00:00.123456+00:00").run()
    assert not app.dataframe and not app.error
    app.text_input(key="start").set_value("bad-time").run()
    assert app.error and not app.dataframe


@pytest.mark.parametrize("config", [{"schema": "gold_m4_final"}, {"namespace": "*"}, {"default_currency": "usd"},
                                    {"refresh_seconds": 0}, {"schema": "public"}])
def test_unsafe_or_nonproduction_config_rejected(config):
    with pytest.raises(ValueError):
        service.DashboardConfig(**config)


def test_streamlit_currency_filters_and_exact_tables(setup, monkeypatch):
    _, _, _, reader = setup
    snapshot = reader.load()
    monkeypatch.setattr(service.DashboardReadService, "load", lambda self: snapshot)
    monkeypatch.setenv("POSTGRES_GOLD_SCHEMA", "gold_serving")
    app = AppTest.from_file(APP).run(timeout=20)
    assert not app.exception and not app.error
    assert {h.value for h in app.subheader} == {"Revenue — INR", "Revenue — USD", "Products — INR", "Products — USD"}
    assert len(app.dataframe) == 4
    assert all(isinstance(x, str) for x in app.dataframe[1].value["revenue"])
    assert any("Gold version: 3" in c.value for c in app.caption)
    app.selectbox(key="currency").select("USD").run()
    assert len(app.dataframe) == 2 and set(app.dataframe[0].value["currency"]) == {"USD"}
    app.selectbox(key="period").select("Last 30 minutes (UTC now)").run()
    assert not app.dataframe and any("No revenue rows" in i.value for i in app.info)


@pytest.mark.parametrize("empty", [False, True])
def test_streamlit_empty_and_error_are_distinct(setup, monkeypatch, empty):
    _, _, _, reader = setup
    snapshot = reader.load()

    def load(self):
        if empty:
            return service.DashboardSnapshot(snapshot.metadata, (), (), "Redis cache", "valid", snapshot.namespace)
        raise service.DashboardUnavailable("unavailable")

    monkeypatch.setattr(service.DashboardReadService, "load", load)
    monkeypatch.setenv("POSTGRES_GOLD_SCHEMA", "gold_serving")
    app = AppTest.from_file(APP).run(timeout=20)
    assert not app.exception and not app.dataframe and not app.metric
    assert bool(app.error) is not empty
    if empty:
        assert any("Valid empty committed snapshot" in i.value for i in app.info)


# ---------------------------------------------------------------------------
# Phase 3B regression: table layout (window_end accessibility) and null display
# ---------------------------------------------------------------------------

def test_display_rows_renders_null_as_em_dash_and_preserves_values():
    row = {"avg_order_value": None, "net_revenue": Decimal("0"), "gross_revenue": Decimal("1234.5")}
    display = service.display_rows([row])[0]
    # A null renders as an em dash, never the raw Python "None".
    assert display["avg_order_value"] == service.NULL_DISPLAY == "\u2014"
    # A genuine zero stays a zero (0.00) and remains distinguishable from null.
    assert display["net_revenue"] == "0.00"
    # Ordinary positive money renders through the shared two-decimal boundary.
    assert display["gross_revenue"] == "1,234.50"
    # The presentation transform never mutates the underlying source values.
    assert row["avg_order_value"] is None
    assert row["net_revenue"] == Decimal("0")
    assert row["gross_revenue"] == Decimal("1234.5")


def test_streamlit_tables_lead_with_window_columns_and_render_null_dash(setup, monkeypatch):
    """Layout + null regression through the real render path.

    These assertions cover the *mechanism* that keeps ``window_end`` accessible at
    1440px (it is a leading, left-anchored column rather than the clipped rightmost
    one) and the null-cell presentation. Actual pixel/DOM confirmation at 1440x900
    and 1920x1080 is done separately with the headless-Chrome QA driver, not here.
    """
    _, _, _, reader = setup
    snapshot = reader.load()
    monkeypatch.setattr(service.DashboardReadService, "load", lambda self: snapshot)
    monkeypatch.setenv("POSTGRES_GOLD_SCHEMA", "gold_serving")
    app = AppTest.from_file(APP).run(timeout=20)
    assert not app.exception and not app.error

    revenue_frame = app.dataframe[0].value  # INR revenue table
    # window_end is present and is a leading column, so the viewport cannot clip it.
    assert "window_end" in revenue_frame.columns
    assert list(revenue_frame.columns[:2]) == ["window_start", "window_end"]
    # The fixture's null avg_order_value renders as an em dash, not "None"; a real
    # value in the same column renders at the shared two-decimal precision.
    avg_cells = set(revenue_frame["avg_order_value"])
    assert service.NULL_DISPLAY in avg_cells and "None" not in avg_cells
    assert "100.00" in avg_cells

    # The product table shared the same root cause and received the same fix.
    product_frame = app.dataframe[1].value
    assert list(product_frame.columns[:2]) == ["window_start", "window_end"]


# ---------------------------------------------------------------------------
# Phase 2 currency presentation: the shared two-decimal display formatter
# ---------------------------------------------------------------------------

def test_format_money_is_the_shared_two_decimal_currency_boundary():
    """Phase 2 polish: KPI/insight money renders at two decimals with separators."""
    assert service.format_money(Decimal("12830.50000005"), "INR") == "INR 12,830.50"
    assert service.format_money(Decimal("18890.00999999"), "USD") == "USD 18,890.01"
    assert service.format_money(Decimal("1234.5"), "INR") == "INR 1,234.50"
    assert service.format_money(Decimal("0.1"), "USD") == "USD 0.10"
    assert service.format_money(Decimal("1500000000.5"), "USD") == "USD 1,500,000,000.50"


def test_format_money_rounds_half_up_without_mutating_the_stored_decimal():
    # Two-decimal display rounding is half-up and presentation-only: the passed
    # Decimal object is never quantized, so callers keep full contract precision.
    stored = Decimal("15330.50000005")
    assert service.format_money(stored, "INR") == "INR 15,330.50"
    assert stored == Decimal("15330.50000005") and stored.as_tuple().exponent == -8
    assert service.format_money(Decimal("0.005"), "INR") == "INR 0.01"
    assert service.format_money(Decimal("-0.005"), "INR") == "INR -0.01"
    assert service.format_money(Decimal("5110.16666668"), "INR") == "INR 5,110.17"


def test_format_money_zero_negative_and_null_are_explicit_and_distinct():
    assert service.format_money(Decimal("0"), "INR") == "INR 0.00"
    assert service.format_money(Decimal("-30.00000001"), "INR") == "INR -30.00"
    null_text = service.format_money(None, "USD")
    assert null_text == "USD — (not applicable)"  # a null never degrades into a zero
    assert null_text != service.format_money(Decimal("0"), "USD")


@pytest.mark.parametrize("invalid", ["12.34", 1.5, True, object()])
def test_format_amount_fails_loudly_on_non_decimal_values(invalid):
    # The renderer receives contract-validated Decimals; anything else must raise
    # instead of being coerced into a plausible-looking amount.
    with pytest.raises(TypeError):
        service.format_amount(invalid)


def test_format_amount_handles_the_contract_maximum_magnitude():
    # DECIMAL(38,8)-shaped values exceed the default decimal context precision;
    # display must not raise InvalidOperation on them.
    assert service.format_amount(Decimal("999999999999999999999999999999.99999999")) == \
        "1,000,000,000,000,000,000,000,000,000,000.00"


def test_display_rows_uses_the_shared_formatter_and_keeps_rows_intact():
    row = {"gross_revenue": Decimal("24999.99999995"), "order_count": 5, "region": "south-india"}
    display = service.display_rows([row])[0]
    assert display["gross_revenue"] == "25,000.00"  # float dust rounded for display only
    assert row["gross_revenue"] == Decimal("24999.99999995")  # the stored value is unchanged
    assert display["order_count"] == 5 and display["region"] == "south-india"


# ---------------------------------------------------------------------------
# P1 isolation regression: mode selection and cache state at test boundaries
# ---------------------------------------------------------------------------


def test_hostile_demo_flag_cannot_reach_a_real_render(setup, monkeypatch):
    """An exported ``OIP_DEMO_MODE=synthetic`` must not move a real render onto demo data.

    ``monkeypatch.setenv`` reproduces what a shell export leaves in the pytest
    process; the isolation boundary is the same one the autouse fixture applies
    before every test, so this asserts the real protection rather than a stub.
    """
    _, _, _, reader = setup
    snapshot = reader.load()
    monkeypatch.setattr(service.DashboardReadService, "load", lambda self: snapshot)
    monkeypatch.setenv(DEMO_MODE_ENV_VAR, DEMO_MODE_VALUE)
    assert selected_mode() == MODE_DEMO  # the contamination is live before isolation
    with real_mode_boundary(monkeypatch):
        assert os.environ.get(DEMO_MODE_ENV_VAR) is None
        assert selected_mode() == MODE_REAL
        app = AppTest.from_file(APP).run(timeout=20)
    assert not app.exception and not app.error
    # Markers only the real source path produces: M5 metadata and the real filters.
    assert app.selectbox(key="period").options == dashboard_app.REAL_PERIOD_OPTIONS
    assert any("Gold version: 3" in caption.value for caption in app.caption)
    assert any(button.label == "Refresh snapshot" for button in app.button)
    assert DEMO_DISCLOSURE not in {warning.value for warning in app.warning}
    assert len(app.dataframe) == 4


def test_unisolated_demo_flag_really_does_change_the_render(monkeypatch):
    """Negative control: without the boundary the flag sends the app down the demo path.

    This documents why the isolation fixture is load-bearing. It asserts the
    production semantics are intentional (an explicit flag wins), not a defect.
    """
    monkeypatch.setenv(DEMO_MODE_ENV_VAR, DEMO_MODE_VALUE)
    app = AppTest.from_file(APP).run(timeout=30)
    assert not app.exception
    assert app.selectbox(key="period").options == dashboard_app.DEMO_PERIOD_OPTIONS
    assert DEMO_DISCLOSURE in {warning.value for warning in app.warning}


def test_isolation_preserves_an_explicit_synthetic_selection(monkeypatch):
    """Clearing the environment must not force demo tests onto the real path."""
    monkeypatch.setenv(DEMO_MODE_ENV_VAR, DEMO_MODE_VALUE)
    with real_mode_boundary(monkeypatch):
        monkeypatch.setenv(DEMO_MODE_ENV_VAR, DEMO_MODE_VALUE)
        assert selected_mode() == MODE_DEMO
        app = AppTest.from_file(APP).run(timeout=30)
    assert not app.exception
    assert app.selectbox(key="period").options == dashboard_app.DEMO_PERIOD_OPTIONS
    assert DEMO_DISCLOSURE in {warning.value for warning in app.warning}
    assert not any("Refresh snapshot" == button.label for button in app.button)


def test_caches_do_not_survive_a_failing_test(setup, monkeypatch):
    """Cleanup runs on the failure path: a raise must not leave a cached service behind.

    The cached ``DashboardReadService`` is built under the failing test's
    monkeypatched class, so handing it to the next test would be cross-test state.
    Counting constructions proves the second session rebuilt rather than reused it.
    """
    _, _, _, reader = setup
    snapshot = reader.load()
    constructions = []
    original_init = service.DashboardReadService.__init__

    def counted(self, config=None, **kwargs):
        constructions.append(config)
        original_init(self, config, **kwargs)

    monkeypatch.setattr(service.DashboardReadService, "__init__", counted)
    monkeypatch.setattr(service.DashboardReadService, "load", lambda self: snapshot)
    with pytest.raises(RuntimeError):
        with real_mode_boundary(monkeypatch):
            failing = AppTest.from_file(APP).run(timeout=20)
            assert len(failing.dataframe) == 4 and len(constructions) == 1
            raise RuntimeError("simulated mid-test failure")
    app = AppTest.from_file(APP).run(timeout=20)
    assert not app.exception and len(app.dataframe) == 4
    assert len(constructions) == 2  # the boundary dropped the cache, so it was rebuilt


def test_demo_then_real_sessions_do_not_leak_into_each_other(setup, monkeypatch):
    """Session order cannot decide the outcome when every render crosses the boundary."""
    _, _, _, reader = setup
    snapshot = reader.load()
    monkeypatch.setattr(service.DashboardReadService, "load", lambda self: snapshot)
    with real_mode_boundary(monkeypatch):
        monkeypatch.setenv(DEMO_MODE_ENV_VAR, DEMO_MODE_VALUE)
        demo = AppTest.from_file(APP).run(timeout=30)
    assert not demo.exception
    assert demo.selectbox(key="period").options == dashboard_app.DEMO_PERIOD_OPTIONS
    assert len(demo.dataframe) == 4
    with real_mode_boundary(monkeypatch):
        assert os.environ.get(DEMO_MODE_ENV_VAR) is None  # the boundary, not the shell
        real = AppTest.from_file(APP).run(timeout=20)
    assert not real.exception
    assert real.selectbox(key="period").options == dashboard_app.REAL_PERIOD_OPTIONS
    assert any("Gold version: 3" in caption.value for caption in real.caption)
