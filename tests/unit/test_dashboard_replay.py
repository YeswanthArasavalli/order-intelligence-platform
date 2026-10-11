"""Scripted synthetic replay tests: deterministic snapshots, progression and honesty.

Self-contained like the fixed-demo suites: no Kafka, Spark, MinIO, PostgreSQL,
Redis, credentials, network or ``.env`` is touched, and real-source client
construction is asserted absent (the ``isolated`` fixture makes any attempt
raise). Tests inject the replay clock or call the pure progression function
with fixed timestamps; nothing sleeps on ``DASHBOARD_REFRESH_SECONDS``.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import importlib
from pathlib import Path
import re

import pytest
from streamlit.testing.v1 import AppTest

from dashboard import app as dashboard_app
from dashboard import demo_provider, read_service as service, replay
from dashboard.demo_provider import (DEMO_DISCLOSURE, DEMO_MODE_ENV_VAR, MODE_DEMO, MODE_INVALID, MODE_REAL,
                                     MODE_REPLAY, REPLAY_MODE_VALUE, selected_mode)
from dashboard.replay import (REPLAY_DISCLOSURE, REPLAY_FINISHED_MESSAGE, REPLAY_REDIS_STATUS, REPLAY_SOURCE_LABEL,
                              REPLAY_STEP_COUNT)
from storage.postgres_redis_serving import KEYS, TABLES, _digest

APP = str(Path(__file__).resolve().parents[2] / "dashboard" / "app.py")
EM = "\u2014"  # em dash used by captions and null rendering
INTERVAL = 10  # the default DASHBOARD_REFRESH_SECONDS of the isolated environment
START = datetime(2026, 10, 10, 9, 0, tzinfo=timezone.utc)

EXPECTED_REVENUE_ROWS = (3, 6, 9, 12)
EXPECTED_PRODUCT_ROWS = (2, 5, 8, 11)


def replay_config():
    return demo_provider.demo_config()


def step_snapshot(step):
    return replay.replay_snapshot(replay_config(), step)


# ---------------------------------------------------------------------------
# flag selection (fail-safe) and configuration errors
# ---------------------------------------------------------------------------

def test_scripted_flag_selects_replay_mode(monkeypatch):
    monkeypatch.setenv(DEMO_MODE_ENV_VAR, REPLAY_MODE_VALUE)
    assert selected_mode() == MODE_REPLAY


@pytest.mark.parametrize("value,expected", [
    ("synthetic", MODE_DEMO), ("scripted", MODE_REPLAY), ("", MODE_REAL),
    ("Scripted", MODE_INVALID), (" scripted", MODE_INVALID), ("scripted ", MODE_INVALID),
    ("replay", MODE_INVALID), ("SCRIPTED", MODE_INVALID)])
def test_mode_selection_covers_every_documented_value(monkeypatch, value, expected):
    monkeypatch.setenv(DEMO_MODE_ENV_VAR, value)
    assert selected_mode() == expected


def test_error_message_names_both_synthetic_values():
    message = demo_provider.mode_error_message("Scripted")
    assert DEMO_MODE_ENV_VAR in message and "Scripted" in message
    assert "synthetic" in message and "scripted" in message


# ---------------------------------------------------------------------------
# snapshot construction: finite predefined sequence, deterministic, contract-valid
# ---------------------------------------------------------------------------

def test_sequence_is_finite_with_documented_row_growth():
    assert REPLAY_STEP_COUNT == 4
    for step in range(REPLAY_STEP_COUNT):
        sample = replay.replay_payload(replay_config(), step)
        assert len(sample["revenue_by_minute"]) == EXPECTED_REVENUE_ROWS[step]
        assert len(sample["product_rankings"]) == EXPECTED_PRODUCT_ROWS[step]
        assert sample["metadata"]["revenue_rows"] == EXPECTED_REVENUE_ROWS[step]
        assert sample["metadata"]["product_rows"] == EXPECTED_PRODUCT_ROWS[step]


def test_out_of_range_steps_are_rejected():
    for step in (-1, REPLAY_STEP_COUNT, 99):
        with pytest.raises(ValueError):
            replay.replay_payload(replay_config(), step)


def test_snapshots_are_deterministic_across_calls_and_reload():
    first = [step_snapshot(step) for step in range(REPLAY_STEP_COUNT)]
    reloaded = importlib.reload(replay)
    try:
        for step, snapshot in enumerate(first):
            other = reloaded.replay_snapshot(replay_config(), step)
            assert other.metadata == snapshot.metadata
            assert other.revenue_by_minute == snapshot.revenue_by_minute
            assert other.product_rankings == snapshot.product_rankings
    finally:
        importlib.reload(replay)


def test_rows_match_the_serving_contract_and_identities_are_unique():
    for step in range(REPLAY_STEP_COUNT):
        sample = replay.replay_payload(replay_config(), step)
        for table in TABLES:
            for row in sample[table]:
                assert set(row) == set(TABLES[table])
            identities = [tuple(row[column] for column in KEYS[table]) for row in sample[table]]
            assert len(set(identities)) == len(identities)


def test_every_step_reconciles_products_to_its_own_revenue_ledger():
    for step in range(REPLAY_STEP_COUNT):
        snapshot = step_snapshot(step)
        for currency in ("INR", "USD"):
            orders = sum(r["order_count"] for r in snapshot.revenue_by_minute if r["currency"] == currency)
            units = sum(r["units_sold"] for r in snapshot.product_rankings if r["currency"] == currency)
            gross = sum((r["gross_revenue"] for r in snapshot.revenue_by_minute if r["currency"] == currency),
                        Decimal(0))
            revenue = sum((r["revenue"] for r in snapshot.product_rankings if r["currency"] == currency),
                          Decimal(0))
            assert orders == units and gross == revenue


def test_final_step_shows_the_complete_fixed_revenue_ledger():
    sample = replay.replay_payload(replay_config(), REPLAY_STEP_COUNT - 1)
    assert sample["revenue_by_minute"] == demo_provider.synthetic_payload(replay_config())["revenue_by_minute"]


def test_metadata_is_a_predefined_increasing_sequence_with_real_digests():
    for step in range(REPLAY_STEP_COUNT):
        metadata = replay.replay_payload(replay_config(), step)["metadata"]
        assert metadata["gold_version"] == demo_provider.GOLD_VERSION + step
        assert metadata["served_at"] == (demo_provider.ANCHOR + timedelta(minutes=step, seconds=30)).isoformat(
            timespec="microseconds")
        for name in ("snapshot_digest", "manifest_digest", "snapshot_identity"):
            assert re.fullmatch(r"[0-9a-f]{64}", metadata[name])
        assert "synthetic" in metadata["source_root"] and "synthetic" in metadata["manifest_path"]
    digests = [replay.replay_payload(replay_config(), step)["metadata"]["snapshot_digest"]
               for step in range(REPLAY_STEP_COUNT)]
    assert len(set(digests)) == REPLAY_STEP_COUNT  # every step is a genuinely different snapshot
    for step in range(REPLAY_STEP_COUNT):
        sample = replay.replay_payload(replay_config(), step)
        assert sample["metadata"]["snapshot_digest"] == _digest(
            {table: sample[table] for table in TABLES})


def test_snapshots_survive_the_wire_round_trip_and_stay_typed():
    snapshot = step_snapshot(2)
    assert snapshot.source == REPLAY_SOURCE_LABEL and snapshot.redis_status == REPLAY_REDIS_STATUS
    assert not snapshot.empty and snapshot.currencies == ["INR", "USD"]
    for row in snapshot.revenue_by_minute:
        assert type(row["net_revenue"]) is Decimal and row["window_start"].tzinfo is timezone.utc
    for row in snapshot.product_rankings:
        assert type(row["revenue"]) is Decimal and type(row["units_sold"]) is int


# ---------------------------------------------------------------------------
# progression semantics: pure clock function (no sleeps, no timing dependence)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("elapsed_seconds,expected", [
    (-5, 0), (0, 0), (INTERVAL - 1, 0), (INTERVAL, 1), (2 * INTERVAL, 2), (3 * INTERVAL, 3),
    (4 * INTERVAL, REPLAY_STEP_COUNT - 1), (1000 * INTERVAL, REPLAY_STEP_COUNT - 1)])
def test_advance_index_progresses_then_stops_without_looping(elapsed_seconds, expected):
    current = START + timedelta(seconds=elapsed_seconds)
    assert replay.advance_index(START, INTERVAL, current=current) == expected


def test_repeated_reads_inside_one_interval_return_the_same_step():
    for tick in (0, 3, 9, INTERVAL - 0.5):
        assert replay.advance_index(START, INTERVAL, current=START + timedelta(seconds=tick)) == 0


def test_replay_module_has_no_real_source_entrypoints():
    exposed = set(dir(replay))
    assert not exposed & {"serve_postgres_snapshot", "delete_cache", "get_redis_client",
                          "get_postgres_engine", "read_postgres_snapshot", "read_active_snapshot"}


# ---------------------------------------------------------------------------
# rendering through the unchanged Streamlit application (injected clock)
# ---------------------------------------------------------------------------

@pytest.fixture()
def isolated(monkeypatch):
    """Clean dashboard environment; any real client construction or use fails loudly."""

    def forbidden(*args, **kwargs):
        raise AssertionError("replay mode must not construct or use real source clients")

    monkeypatch.setattr(service.DashboardReadService, "__init__", forbidden)
    monkeypatch.setattr(service.DashboardReadService, "load", forbidden)
    monkeypatch.setattr(service.redis, "Redis", forbidden)
    monkeypatch.setattr("storage.postgres_redis_serving.create_engine", forbidden)
    for key in (DEMO_MODE_ENV_VAR, "DASHBOARD_SOURCE_TOPIC", "DASHBOARD_DEFAULT_CURRENCY",
                "DASHBOARD_REFRESH_SECONDS", "POSTGRES_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


class Clock:
    """Manually advanced deterministic clock injected at the replay seam."""

    def __init__(self):
        self.instant = START

    def now(self):
        return self.instant

    def to_step(self, step):
        self.instant = START + timedelta(seconds=INTERVAL * step)


def run_replay_app(isolated, step=0):
    """Launch the replay at the anchored start instant (step 0), then advance to ``step``.

    The active step is elapsed-time-since-launch, so advancing mid-session the
    way the browser's refresh ticks do; no test ever sleeps.
    """
    isolated.setenv(DEMO_MODE_ENV_VAR, REPLAY_MODE_VALUE)
    clock = Clock()
    isolated.setattr(replay, "now", clock.now)
    app = AppTest.from_file(APP).run(timeout=30)
    assert not app.exception, [str(exception.value) for exception in app.exception]
    if step:
        clock.to_step(step)
        app.run()
        assert not app.exception, [str(exception.value) for exception in app.exception]
    return app, clock


def caption_text(app):
    return [caption.value for caption in app.caption]


def expected_kpi_values(step):
    """The metrics the render loop must show for the active snapshot's latest windows."""
    snapshot = step_snapshot(step)
    values = []
    for code in ("INR", "USD"):
        revenue = sorted((r for r in snapshot.revenue_by_minute if r["currency"] == code),
                         key=lambda r: (r["window_start"], r["window_end"], r["region"], r["currency"]))
        row = revenue[-1]
        values += [service.format_money(row["net_revenue"], code), str(row["order_count"]),
                   service.format_money(row["avg_order_value"], code)]
    return values


def test_replay_disclosure_and_step_indicator_are_visible(isolated):
    app, _ = run_replay_app(isolated, step=0)
    assert REPLAY_DISCLOSURE in {warning.value for warning in app.warning}
    assert DEMO_DISCLOSURE not in {warning.value for warning in app.warning}
    assert any(f"Replay step 1 of {REPLAY_STEP_COUNT}" in value for value in caption_text(app))
    assert any("no redis or postgresql source was read" in value.lower() for value in caption_text(app))
    assert any("nothing here queries kafka, spark, postgresql or redis" in value.lower()
               for value in caption_text(app))
    assert not [button for button in app.button if button.label == "Refresh snapshot"]
    assert any(button.label == "Restart replay" for button in app.button)
    assert app.selectbox(key="period").options == dashboard_app.DEMO_PERIOD_OPTIONS


def test_replay_advances_the_displayed_snapshot(isolated):
    app, clock = run_replay_app(isolated, step=0)
    assert [metric.value for metric in app.metric] == expected_kpi_values(0)
    assert any(f"Matching rows {EM} revenue: 3, products: 2" in value for value in caption_text(app))
    clock.to_step(2)
    app.run()
    assert not app.exception
    assert any(f"Replay step 3 of {REPLAY_STEP_COUNT}" in value for value in caption_text(app))
    assert any(f"Matching rows {EM} revenue: 9, products: 8" in value for value in caption_text(app))
    assert [metric.value for metric in app.metric] == expected_kpi_values(2)


def test_manual_rerun_and_filters_never_skip_a_step(isolated):
    app, clock = run_replay_app(isolated, step=1)
    first = [metric.value for metric in app.metric]
    app.run()  # manual rerun at the same instant
    assert [metric.value for metric in app.metric] == first
    app.selectbox(key="currency").select("INR").run()  # interaction rerun at the same instant
    assert [metric.value for metric in app.metric] == first[:3]
    assert any(f"Replay step 2 of {REPLAY_STEP_COUNT}" in value for value in caption_text(app))


def test_final_step_is_stable_and_reports_the_stop(isolated):
    app, clock = run_replay_app(isolated, step=REPLAY_STEP_COUNT - 1)
    assert any(f"Replay step {REPLAY_STEP_COUNT} of {REPLAY_STEP_COUNT}" in value for value in caption_text(app))
    assert any(REPLAY_FINISHED_MESSAGE in info.value for info in app.info)
    final = [metric.value for metric in app.metric]
    clock.to_step(50)  # far beyond the sequence: clamped, never loops back to step 1
    app.run()
    assert [metric.value for metric in app.metric] == final
    assert any(f"Replay step {REPLAY_STEP_COUNT} of {REPLAY_STEP_COUNT}" in value for value in caption_text(app))


def test_restart_button_returns_to_the_initial_snapshot(isolated):
    app, clock = run_replay_app(isolated, step=2)
    assert any(f"Replay step 3 of {REPLAY_STEP_COUNT}" in value for value in caption_text(app))
    app.button(key="replay_restart").click().run()
    assert not app.exception
    assert any(f"Replay step 1 of {REPLAY_STEP_COUNT}" in value for value in caption_text(app))
    assert [metric.value for metric in app.metric] == expected_kpi_values(0)


def test_every_step_keeps_the_disclosure_visible(isolated):
    app, clock = run_replay_app(isolated, step=0)
    for step in range(REPLAY_STEP_COUNT):
        clock.to_step(step)
        app.run()
        assert not app.exception
        assert REPLAY_DISCLOSURE in {warning.value for warning in app.warning}


def test_currency_and_region_filters_still_work_on_a_replay_step(isolated):
    app, _ = run_replay_app(isolated, step=REPLAY_STEP_COUNT - 1)
    app.selectbox(key="currency").select("INR").run()
    assert len(app.metric) == 3 and len(app.dataframe) == 2
    assert set(app.dataframe[0].value["currency"]) == {"INR"}
    app.selectbox(key="region").select("north-india").run()
    assert not app.exception
    assert set(app.dataframe[0].value["region"]) == {"north-india"}
    assert len(app.dataframe[1].value) == 8  # products keep their all-region contract (8 INR rows at step 4)


def test_insights_are_recomputed_from_the_active_step(isolated):
    app, clock = run_replay_app(isolated, step=1)
    clock.to_step(3)
    app.run()
    app.selectbox(key="currency").select("INR").run()
    snapshot = step_snapshot(3)
    revenue = [r for r in snapshot.revenue_by_minute if r["currency"] == "INR"]
    totals = {}
    for row in revenue:
        totals[row["region"]] = totals.get(row["region"], Decimal(0)) + row["net_revenue"]
    lead_region, lead_net = min(totals.items(), key=lambda item: (-item[1], item[0]))
    text = "\n".join(markdown.value for markdown in app.markdown)
    assert "Leading region by net revenue (2 in view, all regions)" in text
    assert f"{lead_region} {EM} {service.format_money(lead_net, 'INR')}" in text


def test_empty_and_invalid_ranges_stay_safe_on_a_replay_step(isolated):
    app, _ = run_replay_app(isolated, step=2)
    app.selectbox(key="period").select("Custom UTC range").run()
    app.text_input(key="start").set_value("2020-01-01T00:00:00+00:00")
    app.text_input(key="end").set_value("2020-01-02T00:00:00+00:00").run()
    assert not app.exception and not app.error
    assert not app.metric and not app.dataframe
    assert any(f"Matching rows {EM} revenue: 0, products: 0" in value for value in caption_text(app))
    app.text_input(key="start").set_value("not-a-timestamp")
    app.run()
    assert any("iso-8601" in error.value.lower() for error in app.error)


def test_null_average_order_value_stays_distinguishable_from_zero(isolated):
    # Step 0: international INR-less minute has zero paid events, so the USD KPI row is null.
    app, _ = run_replay_app(isolated, step=0)
    assert app.metric[5].value == f"USD {EM} (not applicable)"
    usd_revenue = app.dataframe[2].value  # INR revenue, INR products, USD revenue, USD products
    assert list(usd_revenue["avg_order_value"]) == [EM]
    assert list(usd_revenue["net_revenue"]) == ["0.00"]  # genuine zero renders as digits, not the dash


@pytest.mark.parametrize("flag", ["Scripted", " scripted", "scripted "])
def test_invalid_scripted_variants_stop_before_any_real_source(isolated, flag):
    isolated.setenv(DEMO_MODE_ENV_VAR, flag)
    app = AppTest.from_file(APP).run(timeout=30)
    assert not app.exception
    assert any(DEMO_MODE_ENV_VAR in error.value and flag in error.value for error in app.error)
    assert not app.metric and not app.dataframe and not app.selectbox
    assert REPLAY_DISCLOSURE not in {warning.value for warning in app.warning}


def test_fixed_demo_mode_is_unchanged_by_the_replay_addition(isolated):
    isolated.setenv(DEMO_MODE_ENV_VAR, "synthetic")
    app = AppTest.from_file(APP).run(timeout=30)
    assert not app.exception
    assert DEMO_DISCLOSURE in {warning.value for warning in app.warning}
    assert REPLAY_DISCLOSURE not in {warning.value for warning in app.warning}
    assert not any("replay step" in value.lower() for value in caption_text(app))
    assert not [button for button in app.button if button.label == "Restart replay"]
    assert len(app.metric) == 6 and len(app.dataframe) == 4 and len(app.get("plotly_chart")) == 2
    assert [metric.value for metric in app.metric] == [
        "INR 12,830.50", "3", "INR 5,110.17",
        "USD 18,890.01", "3", "USD 6,710.00"]
