"""Scripted synthetic replay for the M6 dashboard (``OIP_DEMO_MODE=scripted``).

The replay steps through a finite, predefined sequence of four synthetic
snapshots so the dashboard visibly changes without any live pipeline. The
sequence is the fixed demo ledger (``dashboard/demo_provider.py``) revealed one
minute at a time: step ``i`` commits revenue minute ``i`` on top of the previous
steps and recomputes the same five-minute product window from the ledger seen so
far, mirroring the Gold contract's revisable full-history snapshots
(``streaming/GOLD_CONTRACT.md``). Nothing is generated from randomness or from
the clock's date; the only time dependence is *which* predefined step is active,
derived from elapsed session time on the existing ``st.fragment(run_every=...)``
reruns — no extra thread, timer or service.

Replay semantics (documented here and pinned by tests):

* initial snapshot: opening the replay shows step 1 of 4;
* progression: the active step advances by one for every full
  ``DASHBOARD_REFRESH_SECONDS`` interval elapsed since the replay started;
* final snapshot: the sequence stops at step 4 and stays there — it never loops;
* manual reruns and filter interactions recompute the same step from the clock,
  so they never skip a snapshot ahead;
* reset: the "Restart replay" button re-anchors the start time and returns to
  step 1.

Honesty boundaries are identical to the fixed demo: every value is synthetic.
``served_at``/``gold_version`` form a fixed predefined per-step sequence,
``manifest_digest``/``snapshot_identity`` are placeholder digests over fixed
labels, and ``snapshot_digest`` is the genuine contract digest over each step's
rows. Each snapshot still survives the real M5 encode → decode → M6 validate
chain. This module constructs no Redis or PostgreSQL client, and real mode
never falls back to it.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from dashboard import demo_provider
from dashboard.demo_provider import (DEMO_SOURCE_ROOT, DEMO_TOPIC, GOLD_VERSION, ANCHOR, demo_config,
                                     _placeholder_digest)
from dashboard.read_service import DashboardSnapshot, _typed
from storage.postgres_redis_serving import _decode, _digest, _fields, _row, _utc

REPLAY_STEP_COUNT = 4
REPLAY_DISCLOSURE = "Scripted synthetic replay — predefined sample snapshots; not live production data."
REPLAY_SOURCE_LABEL = "Synthetic replay data"
REPLAY_REDIS_STATUS = "not used (replay)"
REPLAY_FINISHED_MESSAGE = ("Scripted replay reached the final predefined snapshot and stays there; it does not "
                           "loop. Use Restart replay to watch the sequence again.")

# One fixed product pays each committed revenue minute, so every step's product
# totals tie exactly to that step's paid events (units_sold = order_count,
# revenue = gross_revenue per row). IDs/names come from the producer catalog
# (producer/order_generator.py). product_id+currency is unique per currency
# across all four minutes because every step shares one five-minute product
# window, keeping the stored row identity (KEYS["product_rankings"]) valid.
# Regions with zero paid events in a minute legitimately get no product row.
_STEP_PRODUCTS = {
    (0, "north-india"): ("prod-1001", "Wireless Headphones"),
    (0, "south-india"): ("prod-1002", "Smartphone Case"),
    (1, "north-india"): ("prod-1003", "USB-C Cable"),
    (1, "south-india"): ("prod-1006", "Running Shoes"),
    (1, "international"): ("prod-1007", "Cotton T-Shirt"),
    (2, "north-india"): ("prod-1010", "Yoga Mat"),
    (2, "south-india"): ("prod-1014", "Mechanical Keyboard"),
    (2, "international"): ("prod-1012", "Protein Powder"),
    (3, "north-india"): ("prod-1011", "Face Wash"),
    (3, "south-india"): ("prod-1005", "Bluetooth Speaker"),
    (3, "international"): ("prod-1015", "Monitor (24 inch)"),
}


def now() -> datetime:
    """Single clock seam for the replay; tests inject a deterministic clock here."""
    return datetime.now(timezone.utc)


def advance_index(started_at: datetime, interval_seconds: int, current: datetime | None = None) -> int:
    """Derive the active step from elapsed time: stable, clamped, never looping.

    Pure over its clock arguments, so progression is testable without sleeping;
    repeated calls inside one interval return the same step (manual reruns and
    filter interactions never skip ahead), and the result stays on the final
    step forever once the sequence is complete.
    """
    current = now() if current is None else current
    elapsed = (current - started_at).total_seconds()
    if elapsed <= 0:
        return 0
    return min(int(elapsed // interval_seconds), REPLAY_STEP_COUNT - 1)


def step_served_at(step: int) -> datetime:
    """Fixed predefined commit timestamp for one step (never the real clock)."""
    return ANCHOR + timedelta(minutes=step, seconds=30)


def _revenue_rows(step: int) -> list[dict]:
    """The fixed ledger rows whose minute offset is committed by this step."""
    return [row for row, spec in zip(demo_provider._revenue_rows(), demo_provider._REVENUE_SPEC)
            if spec[0] <= step]


def _product_rows(step: int) -> list[dict]:
    rows = []
    for offset, region, currency, orders, _failed, _refunded, gross, _refund, _cancel in demo_provider._REVENUE_SPEC:
        if offset > step or not orders:
            continue
        product_id, product_name = _STEP_PRODUCTS[(offset, region)]
        rows.append(_row({"window_start": ANCHOR, "window_end": ANCHOR + timedelta(minutes=5),
                          "product_id": product_id, "product_name": product_name, "currency": currency,
                          "units_sold": orders, "revenue": Decimal(gross)}, "product_rankings"))
    return rows


def replay_payload(config, step: int) -> dict:
    """Canonical serving-contract payload for one predefined replay step."""
    if not 0 <= step < REPLAY_STEP_COUNT:
        raise ValueError(f"Replay step must be between 0 and {REPLAY_STEP_COUNT - 1}")
    tables = {"revenue_by_minute": _revenue_rows(step), "product_rankings": _product_rows(step)}
    version = GOLD_VERSION + step
    metadata = {
        "source_topic": DEMO_TOPIC, "source_root": DEMO_SOURCE_ROOT, "gold_contract_version": "1.0",
        "gold_version": version, "manifest_path": f"{DEMO_SOURCE_ROOT}/manifests/{version:08d}.json",
        "manifest_digest": _placeholder_digest(f"replay-manifest:{step}"),
        "snapshot_identity": _placeholder_digest(f"replay-snapshot:{step}"),
        "snapshot_digest": _digest(tables), "revenue_rows": len(tables["revenue_by_minute"]),
        "product_rows": len(tables["product_rankings"]), "served_at": _utc(step_served_at(step)),
        "source_database": config.database, "source_schema": config.schema, "cache_contract_version": "1.0",
        "serving_status": "SERVED",
    }
    return {"metadata": metadata, **tables}


def replay_snapshot(config=None, step: int = 0) -> DashboardSnapshot:
    """Validated replay snapshot through the real encode → decode → typed chain.

    Identical guarantee to the fixed demo sample: the step survives the actual
    Redis wire forms and the unchanged M6 consumer validation (metadata
    identity, counts, digests), with synthetic per-step provenance labels.
    """
    config = config or demo_config()
    payload = replay_payload(config, step)
    decoded = _decode(_fields(payload))
    if sorted(decoded) != sorted(payload) or decoded["metadata"] != payload["metadata"]:
        raise AssertionError("Replay payload did not survive the serving-contract round trip")
    for table in demo_provider.TABLES:
        if list(decoded[table]) != list(payload[table]):
            raise AssertionError("Replay rows did not survive the serving-contract round trip")
    return _typed(decoded, REPLAY_SOURCE_LABEL, REPLAY_REDIS_STATUS, config)


def replay_snapshots(config=None) -> tuple[DashboardSnapshot, ...]:
    """The whole finite predefined sequence, in deterministic order."""
    config = config or demo_config()
    return tuple(replay_snapshot(config, step) for step in range(REPLAY_STEP_COUNT))
