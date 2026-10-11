"""Streamlit Community Cloud entry-point tests: guaranteed synthetic mode, no clients.

Self-contained: no Kafka, Spark, MinIO, PostgreSQL, Redis, credentials, network
or ``.env`` file is touched. The entry script runs through the same
``AppTest`` harness the demo suite uses; real-source client construction is
asserted absent, and the ``OIP_DEMO_MODE`` value the script writes into the
process environment is always restored so sibling suites see the ambient state.
One subprocess test additionally proves the entrypoint's ``sys.path`` workaround
behaviorally: it runs the real script from a neutral interpreter with the
repository root removed and ``PYTHONPATH`` cleared, so ``dashboard`` imports
only because the entrypoint re-inserts the root.
"""

from pathlib import Path
import os
import re
import subprocess
import sys
import textwrap

import pytest
from streamlit.testing.v1 import AppTest

from dashboard import read_service as service
from dashboard.demo_provider import DEMO_DISCLOSURE, DEMO_MODE_ENV_VAR, DEMO_MODE_VALUE
from dashboard.replay import REPLAY_DISCLOSURE

ROOT = Path(__file__).resolve().parents[2]
CLOUD_APP = str(ROOT / "streamlit_cloud" / "streamlit_app.py")


@pytest.fixture()
def isolated(monkeypatch):
    """Clean dashboard environment; client construction fails loudly; flag restored.

    The entry script pins ``OIP_DEMO_MODE`` by writing the process environment,
    which ``monkeypatch`` cannot observe at setup time, so the fixture snapshots
    and restores the ambient value itself.
    """
    original = os.environ.get(DEMO_MODE_ENV_VAR)

    def forbidden(*args, **kwargs):
        raise AssertionError("the hosted demo must not construct or use real source clients")

    monkeypatch.setattr(service.DashboardReadService, "__init__", forbidden)
    monkeypatch.setattr(service.DashboardReadService, "load", forbidden)
    monkeypatch.setattr(service.redis, "Redis", forbidden)
    monkeypatch.setattr("storage.postgres_redis_serving.create_engine", forbidden)
    for key in (DEMO_MODE_ENV_VAR, "DASHBOARD_SOURCE_TOPIC", "DASHBOARD_DEFAULT_CURRENCY",
                "DASHBOARD_REFRESH_SECONDS", "POSTGRES_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    yield monkeypatch
    if original is None:
        os.environ.pop(DEMO_MODE_ENV_VAR, None)
    else:
        os.environ[DEMO_MODE_ENV_VAR] = original


def run_cloud_app(isolated):
    app = AppTest.from_file(CLOUD_APP).run(timeout=30)
    assert not app.exception, [str(exception.value) for exception in app.exception]
    return app


# ---------------------------------------------------------------------------
# the hosted demo is always synthetic, whatever the ambient flag was
# ---------------------------------------------------------------------------

def test_cloud_entry_selects_synthetic_when_flag_is_unset(isolated):
    assert DEMO_MODE_ENV_VAR not in os.environ
    app = run_cloud_app(isolated)
    assert os.environ[DEMO_MODE_ENV_VAR] == DEMO_MODE_VALUE
    assert DEMO_DISCLOSURE in {warning.value for warning in app.warning}
    assert not app.error


@pytest.mark.parametrize("ambient", ["", "scripted", "Synthetic", "real", "0"])
def test_cloud_entry_pins_synthetic_over_any_ambient_flag(isolated, ambient):
    """The hosted guarantee: the entry never delegates mode to the environment.

    An unset flag would otherwise select the real path, ``scripted`` a different
    mode, and ``Synthetic``/``real``/``0`` the fail-fast configuration error.
    Every case must end on the fixed synthetic demo with its disclosure.
    """
    isolated.setenv(DEMO_MODE_ENV_VAR, ambient)
    app = run_cloud_app(isolated)
    assert os.environ[DEMO_MODE_ENV_VAR] == DEMO_MODE_VALUE
    warnings = {warning.value for warning in app.warning}
    assert DEMO_DISCLOSURE in warnings
    assert REPLAY_DISCLOSURE not in warnings
    assert not app.error


# ---------------------------------------------------------------------------
# startup touches no external service
# ---------------------------------------------------------------------------

def test_cloud_entry_renders_full_demo_without_any_real_client(isolated):
    """The forbidden-constructor fixture would raise on Redis/SQLAlchemy/read use.

    Reaching the complete synthetic page (6 KPI metrics, 4 tables) therefore
    proves startup and rendering need no PostgreSQL, Redis, Kafka or Spark
    client and read no serving source.
    """
    app = run_cloud_app(isolated)
    assert len(app.metric) == 6 and len(app.dataframe) == 4
    assert any("no redis or postgresql source was read" in caption.value.lower() for caption in app.caption)


# ---------------------------------------------------------------------------
# delegation, not duplication
# ---------------------------------------------------------------------------

def test_cloud_entry_delegates_to_the_existing_dashboard():
    source = Path(CLOUD_APP).read_text(encoding="utf-8")
    code = source.split('"""', 2)[2]
    assert "from dashboard.app import main" in code
    assert "main()" in code
    assert not re.search(r"\bst\.", code)
    assert code.count("from dashboard") == 2  # demo_provider constants + app.main, nothing else


# ---------------------------------------------------------------------------
# Cloud dependency arrangement
# ---------------------------------------------------------------------------

def test_cloud_requirements_are_a_single_pointer_to_the_demo_pins():
    lines = [line.strip() for line in
             (ROOT / "streamlit_cloud" / "requirements.txt").read_text(encoding="utf-8").splitlines()
             if line.strip() and not line.strip().startswith("#")]
    assert lines == ["-r ../requirements-demo.txt"]


def test_demo_pins_cover_the_dashboard_import_chain_and_are_exact():
    entries = [line.split("#")[0].strip() for line in
               (ROOT / "requirements-demo.txt").read_text(encoding="utf-8").splitlines()
               if line.strip() and not line.strip().startswith("#")]
    assert all("==" in entry for entry in entries)
    pins = {entry.split("==")[0] for entry in entries}
    assert {"streamlit", "plotly", "pandas", "sqlalchemy", "redis"} <= pins


# ---------------------------------------------------------------------------
# the sys.path workaround, proven behaviorally (never by matching source text)
# ---------------------------------------------------------------------------

# Run in a fresh interpreter launched from a neutral directory. The bootstrap
# removes the repository root from sys.path, clears the ambient flag, blocks
# every outbound socket, and lets the entrypoint really import dashboard.app (so
# the root re-insertion is exercised) while binding its main to a sentinel so no
# Streamlit render runs. It prints a machine-readable verdict and exits.
_BOOTSTRAP = textwrap.dedent(
    """
    import argparse
    import builtins
    import os
    import runpy
    import socket
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument("--root")
    parser.add_argument("--entry")
    args = parser.parse_args()
    ROOT = os.path.normcase(os.path.realpath(os.path.abspath(args.root)))

    def _is_root(path):
        try:
            return os.path.normcase(os.path.realpath(os.path.abspath(path or os.getcwd()))) == ROOT
        except OSError:
            return False

    # Neutral starting point: no repository root, even if a .pth or cwd added it.
    sys.path = [entry for entry in sys.path if not _is_root(entry)]

    # Controlled harness: the synthetic entrypoint must never touch the network.
    def _blocked(*_args, **_kwargs):
        raise AssertionError("subprocess attempted a real network connection")

    socket.socket.connect = _blocked
    socket.socket.connect_ex = _blocked
    socket.create_connection = _blocked

    # Import guard: really import dashboard.app (this is the path under test) but
    # bind its main to a sentinel so the render never runs.
    _real_import = builtins.__import__
    _state = {"app_imported": False, "main_called": False}

    def _guarded(name, *import_args, **import_kwargs):
        module = _real_import(name, *import_args, **import_kwargs)
        if name == "dashboard.app":
            _state["app_imported"] = True

            def _sentinel_main(*_a, **_k):
                _state["main_called"] = True

            module.main = _sentinel_main
        return module

    builtins.__import__ = _guarded

    try:
        runpy.run_path(args.entry, run_name="__main__")
    except ModuleNotFoundError as exc:
        print("OIP_RESULT=module-not-found")
        print("OIP_MISSING=" + str(exc.name))
        sys.exit(3)

    print("OIP_RESULT=ok")
    print("OIP_APP_IMPORTED=" + str(_state["app_imported"]))
    print("OIP_MAIN_SENTINEL=" + str(_state["main_called"]))
    print("OIP_DEMO_MODE=" + os.environ.get("OIP_DEMO_MODE", ""))
    sys.exit(0)
    """
)

_ROOT_INSERTION = "if str(ROOT) not in sys.path:\n    sys.path.insert(0, str(ROOT))"


def _neutral_env():
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.pop(DEMO_MODE_ENV_VAR, None)
    return env


def _run_entry_in_neutral_subprocess(entry_path, workdir):
    return subprocess.run(
        [sys.executable, "-c", _BOOTSTRAP, "--root", str(ROOT), "--entry", str(entry_path)],
        cwd=str(workdir),
        env=_neutral_env(),
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_cloud_entry_reinserts_the_repo_root_and_imports_dashboard(isolated, tmp_path):
    """The real entrypoint, executed with the repo root absent from sys.path.

    ``dashboard`` is not installed in the interpreter, the working directory is
    an out-of-tree temp path, ``PYTHONPATH`` is cleared and the root is filtered
    out of the subprocess's initial ``sys.path`` — so reaching the dashboard and
    the sentinel proves the entrypoint prepended the root itself and imported
    ``dashboard.app`` without ``ModuleNotFoundError``, with no render and no
    socket. Removing the root insertion from the entrypoint turns this red.
    """
    workdir = tmp_path / "neutral"
    workdir.mkdir()
    assert ROOT not in {parent.resolve() for parent in workdir.resolve().parents}
    entry = ROOT / "streamlit_cloud" / "streamlit_app.py"

    result = _run_entry_in_neutral_subprocess(entry, workdir)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "OIP_RESULT=ok" in result.stdout
    assert "OIP_APP_IMPORTED=True" in result.stdout
    assert "OIP_MAIN_SENTINEL=True" in result.stdout
    assert f"OIP_DEMO_MODE={DEMO_MODE_VALUE}" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


def test_cloud_entry_import_failure_is_caused_by_the_missing_root_insertion(isolated, tmp_path):
    """Negative control: delete only the ``sys.path.insert`` and prove it breaks.

    The same neutral harness must fail with ``ModuleNotFoundError`` for
    ``dashboard`` once the insertion is removed, showing the passing test above
    is attributable to the entrypoint's workaround rather than ambient state.
    """
    source = Path(CLOUD_APP).read_text(encoding="utf-8")
    assert _ROOT_INSERTION in source, "entrypoint no longer contains the insertion to negate"
    mutated = source.replace(_ROOT_INSERTION, "pass  # root-path insertion removed for the control")

    workdir = tmp_path / "neutral"
    workdir.mkdir()
    mutant_entry = workdir / "streamlit_app_noinsert.py"
    mutant_entry.write_text(mutated, encoding="utf-8")

    result = _run_entry_in_neutral_subprocess(mutant_entry, workdir)

    assert result.returncode == 3, result.stdout + result.stderr
    assert "OIP_RESULT=module-not-found" in result.stdout
    assert "OIP_MISSING=dashboard" in result.stdout
    assert "OIP_APP_IMPORTED=True" not in result.stdout
