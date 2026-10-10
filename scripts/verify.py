"""
Self-contained verification entry point: ``python main.py verify``
==================================================================
Runs the shipped unit suites that require **no** Docker services, credentials
or network beyond a local interpreter (Kafka, Spark cluster, MinIO,
PostgreSQL, Redis are never contacted):

- ``tests/unit/test_milestone2.py``   DQ/Silver/DLQ checks on local Spark
- ``tests/unit/test_gold.py``         Gold aggregation on local Parquet/manifests
- ``tests/unit/test_dashboard.py``    M6 read-contract and Streamlit-render checks
- ``tests/unit/test_dashboard_demo.py`` synthetic demo mode contract/render checks
- ``tests/unit/test_dashboard_phase2c.py`` Phase 2C filter-interaction and data-derived insight checks
- ``tests/unit/test_dashboard_replay.py`` scripted synthetic replay snapshot/progression/render checks
- ``tests/unit/test_observability.py`` M9 metrics/protocol checks on loopback HTTP
- ``tests/unit/test_verify.py``       tests for this dispatcher itself

Suites that require the running local serving stack (``test_alerts``,
``test_notifications``, ``test_postgres_serving``, ``test_redis_serving``)
are declared out of scope here and reported as such — they are not run and
not silently counted as passed.

Boundary: this command verifies application code paths only. It is **not**
an infrastructure health check (that is ``make verify``, which probes the
health endpoints of running containers and does not validate pipeline
correctness), and it is **not** proof of a full end-to-end streaming run.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]

SELF_CONTAINED_SUITES: tuple[str, ...] = (
    "tests/unit/test_milestone2.py",
    "tests/unit/test_gold.py",
    "tests/unit/test_dashboard.py",
    "tests/unit/test_dashboard_demo.py",
    "tests/unit/test_dashboard_phase2c.py",
    "tests/unit/test_dashboard_replay.py",
    "tests/unit/test_observability.py",
    "tests/unit/test_verify.py",
)

SERVICE_DEPENDENT_SUITES: tuple[str, ...] = (
    "tests/unit/test_alerts.py",
    "tests/unit/test_notifications.py",
    "tests/unit/test_postgres_serving.py",
    "tests/unit/test_redis_serving.py",
)


def suite_paths(suites: tuple[str, ...]) -> list[str]:
    """Resolve declared suite paths against the repository root, fail fast on typos."""
    paths = []
    for relative in suites:
        path = ROOT / relative
        if not path.is_file():
            raise FileNotFoundError(f"declared test suite is missing: {relative}")
        paths.append(str(path))
    return paths


def _pin_pyspark_interpreter() -> None:
    # PySpark spawns workers through an executable literally named "python3",
    # which does not exist on a default Windows PATH. Pin both roles to the
    # interpreter already running this process so local Spark uses one Python.
    if os.name == "nt":
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def run() -> None:
    _pin_pyspark_interpreter()
    print("Verification scope: self-contained unit suites only.")
    print("No Docker, Kafka, Spark cluster, MinIO, PostgreSQL or Redis services")
    print("are contacted by this command.\n")
    code = pytest.main(["-q", *suite_paths(SELF_CONTAINED_SUITES)])
    print("\nOut of scope (not run, not counted): the service-dependent suites")
    for relative in SERVICE_DEPENDENT_SUITES:
        print(f"  - {relative} (requires the local PostgreSQL/Redis stack)")
    print("See README 'Testing and verification' for how to run those.")
    raise SystemExit(code)


if __name__ == "__main__":
    run()
