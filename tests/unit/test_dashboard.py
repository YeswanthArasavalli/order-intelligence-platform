"""M6 read-contract and actual Streamlit-render tests with independent fixtures."""

import base64
import copy
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import pytest
import redis
from sqlalchemy.exc import OperationalError
from streamlit.testing.v1 import AppTest

from dashboard import read_service as service
from scripts.verify_milestone5 import canonical

APP = str(Path(__file__).resolve().parents[2] / "dashboard" / "app.py")


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
    assert service.format_money(got.revenue_by_minute[0]["net_revenue"], "INR") == "INR -30.00000001"
    assert service.display_rows(got.product_rankings)[0]["revenue"] == "999999999999999999999999999999.99999999"


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
