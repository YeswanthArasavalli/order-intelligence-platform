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
A second subprocess proves the demo flag is already pinned by the time
``dashboard.app`` is imported, that the hosted startup really needs ``redis``
as an import-time dependency, and that it opens no socket.
The dependency-declaration tests compute the third-party modules the hosted
startup chain imports at module load and require every one of them to be
declared in ``requirements-demo.txt``.
"""

import ast
from collections import deque
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


# ---------------------------------------------------------------------------
# the flag is pinned before the dashboard application module is imported,
# and the import needs ``redis`` without ever connecting to a Redis service
# ---------------------------------------------------------------------------

# Fresh interpreter, repository root removed from ``sys.path`` and every outbound
# connection blocked. The mode value is sampled at the moment ``dashboard.app``
# is first imported (before its body runs), ``redis`` importability and the
# connect-attempt count are reported, and ``main`` is bound to a sentinel so no
# render executes.
_MODE_HARNESS = textwrap.dedent(
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

    sys.path = [entry for entry in sys.path if not _is_root(entry)]

    connect_attempts = []

    def _blocked(*_args, **_kwargs):
        connect_attempts.append(1)
        raise AssertionError("subprocess attempted a real network connection")

    socket.socket.connect = _blocked
    socket.socket.connect_ex = _blocked
    socket.create_connection = _blocked

    _real_import = builtins.__import__
    _state = {"mode_at_app_import": "<never imported>", "app_imported": False}

    def _guarded(name, *import_args, **import_kwargs):
        if name == "dashboard.app" and not _state["app_imported"]:
            _state["app_imported"] = True
            _state["mode_at_app_import"] = os.environ.get("OIP_DEMO_MODE", "<unset>")
        module = _real_import(name, *import_args, **import_kwargs)
        if name == "dashboard.app":
            def _sentinel_main(*_a, **_k):
                return None

            module.main = _sentinel_main
        return module

    builtins.__import__ = _guarded

    runpy.run_path(args.entry, run_name="__main__")

    print("OIP_RESULT=ok")
    print("OIP_MODE_AT_APP_IMPORT=" + _state["mode_at_app_import"])
    print("OIP_APP_IMPORTED=" + str(_state["app_imported"]))
    print("OIP_REDIS_IMPORTED=" + str("redis" in sys.modules))
    print("OIP_CONNECT_ATTEMPTS=" + str(len(connect_attempts)))
    """
)

# The exact source block that orders the flag write ahead of the application
# import; the control test below reorders it to prove the harness is sensitive.
_MODE_BEFORE_IMPORT_BLOCK = (
    "os.environ[DEMO_MODE_ENV_VAR] = DEMO_MODE_VALUE\n\nfrom dashboard.app import main  # noqa: E402"
)


def _run_mode_harness(entry_path, workdir):
    return subprocess.run(
        [sys.executable, "-c", _MODE_HARNESS, "--root", str(ROOT), "--entry", str(entry_path)],
        cwd=str(workdir),
        env=_neutral_env(),
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_cloud_entry_pins_synthetic_before_importing_the_dashboard_app(tmp_path):
    """Behavioral ordering guarantee: the flag is set when ``dashboard.app`` loads.

    ``redis`` is proven to be a genuine import-time dependency of the hosted
    startup (it is in ``sys.modules`` afterwards) while the socket guard proves
    the synthetic startup opens no connection, so no Redis service is required.
    """
    workdir = tmp_path / "neutral"
    workdir.mkdir()
    assert ROOT not in {parent.resolve() for parent in workdir.resolve().parents}

    result = _run_mode_harness(ROOT / "streamlit_cloud" / "streamlit_app.py", workdir)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "OIP_APP_IMPORTED=True" in result.stdout
    assert "OIP_MODE_AT_APP_IMPORT=synthetic" in result.stdout
    assert "OIP_REDIS_IMPORTED=True" in result.stdout
    assert "OIP_CONNECT_ATTEMPTS=0" in result.stdout


def test_synthetic_mode_before_import_is_load_bearing(isolated, tmp_path):
    """Negative control: move the flag write after the app import and see it break.

    The same harness must report ``<unset>`` for the mode observed at
    ``dashboard.app`` import time, so the passing ordering above is attributable
    to the entrypoint's placement of the assignment rather than to ambient state.
    """
    source = Path(CLOUD_APP).read_text(encoding="utf-8")
    assert _MODE_BEFORE_IMPORT_BLOCK in source, "entrypoint no longer contains the ordering to negate"
    mutated = source.replace(
        _MODE_BEFORE_IMPORT_BLOCK,
        "from dashboard.app import main  # noqa: E402\n\n"
        "os.environ[DEMO_MODE_ENV_VAR] = DEMO_MODE_VALUE",
    )
    # The mutant lives outside the repository, so its own ``parents[1]`` is not the
    # root. Point that expression at the real root to keep the root insertion
    # equivalent and make the flag ordering the only difference.
    mutated = mutated.replace(
        "ROOT = Path(__file__).resolve().parents[1]",
        f"ROOT = Path({str(ROOT)!r})",
    )
    assert mutated != source and f"ROOT = Path({str(ROOT)!r})" in mutated

    workdir = tmp_path / "neutral"
    workdir.mkdir()
    mutant_entry = workdir / "streamlit_app_late_flag.py"
    mutant_entry.write_text(mutated, encoding="utf-8")

    result = _run_mode_harness(mutant_entry, workdir)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "OIP_APP_IMPORTED=True" in result.stdout
    assert "OIP_MODE_AT_APP_IMPORT=<unset>" in result.stdout


# ---------------------------------------------------------------------------
# every import-time dependency of the hosted startup chain is declared
# ---------------------------------------------------------------------------

# Import names whose distribution name differs; anything else is compared to its
# own lower-cased top-level name.
_DISTRIBUTION_ALIASES = {
    "dateutil": "python-dateutil",
    "psycopg2_binary": "psycopg2-binary",
    "yaml": "pyyaml",
}

# Modules owned by the local pipeline stack. They must stay out of the hosted
# install set: a missing import is fixed by declaring that one package, never by
# pulling PySpark/Kafka/MinIO/PostgreSQL clients into the demo environment.
_PIPELINE_MODULES = frozenset({
    "pyspark", "kafka", "confluent_kafka", "boto3", "botocore", "minio", "psycopg2",
    "faker", "fastavro", "prometheus_client", "pydantic", "structlog", "dotenv",
})


def _repo_file_for(dotted: str) -> Path | None:
    """The repository file a dotted module name resolves to, or None if external."""
    parts = dotted.split(".")
    package = ROOT.joinpath(*parts)
    if (package / "__init__.py").is_file():
        return package / "__init__.py"
    module = package.with_suffix(".py")
    return module if module.is_file() else None


def _module_level_targets(path: Path) -> set[str]:
    """Dotted module names imported by ``path`` at module load (top-level only).

    Function- and ``__main__``-level imports are excluded deliberately: they do
    not run when the hosted entrypoint imports the dashboard, so requiring them
    would over-declare the hosted environment.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    package_parts = path.relative_to(ROOT).parent.parts
    targets: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            targets.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                base = node.module
                targets.add(base)
                targets.update(f"{base}.{alias.name}" for alias in node.names)
            else:
                anchor = package_parts[: len(package_parts) - (node.level - 1)]
                prefix = ".".join(p for p in (*anchor, node.module) if p)
                targets.add(prefix)
                targets.update(f"{prefix}.{alias.name}" for alias in node.names)
    return {target for target in targets if target}


def _startup_chain_files(entry: Path) -> set[Path]:
    """Every repository module reachable through module-level imports of ``entry``."""
    visited: set[Path] = set()
    queue = deque([entry])
    while queue:
        path = queue.popleft()
        if path in visited:
            continue
        visited.add(path)
        for dotted in _module_level_targets(path):
            target = _repo_file_for(dotted)
            if target is not None:
                queue.append(target)
    return visited


def _startup_third_party_imports(entry: Path | None = None) -> tuple[set[Path], set[str]]:
    """(startup-chain files, third-party top-level import names) for the hosted path."""
    entry = entry or Path(CLOUD_APP)
    files = _startup_chain_files(entry)
    imports: set[str] = set()
    for path in files:
        for dotted in _module_level_targets(path):
            top = dotted.split(".")[0]
            if _repo_file_for(top) is not None or top in sys.stdlib_module_names:
                continue
            imports.add(top)
    return files, imports


def _declared_distributions(lines: list[str]) -> set[str]:
    return {line.split("==")[0].strip().lower() for line in lines if "==" in line}


def _demo_pin_lines() -> list[str]:
    text = (ROOT / "requirements-demo.txt").read_text(encoding="utf-8")
    return [line.split("#")[0].strip() for line in text.splitlines()
            if line.strip() and not line.strip().startswith("#")]


def _undeclared(imports: set[str], declared: set[str]) -> set[str]:
    """Import names with no matching pin, comparing normalized distribution names."""
    missing = set()
    for name in imports:
        distribution = _DISTRIBUTION_ALIASES.get(name, name).lower().replace("_", "-")
        if distribution not in declared:
            missing.add(name)
    return missing


def test_hosted_startup_imports_are_all_declared_in_the_demo_pins():
    """The dependency contract behind ``ModuleNotFoundError`` on Community Cloud.

    The scan must reach the real read path (``dashboard/read_service.py`` and
    ``storage/postgres_redis_serving.py``) and then every third-party module it
    imports at module load has to be declared in ``requirements-demo.txt``,
    which is the only pin list the hosted build installs.
    """
    files, imports = _startup_third_party_imports()
    assert ROOT / "dashboard" / "read_service.py" in files
    assert ROOT / "storage" / "postgres_redis_serving.py" in files
    assert ROOT / "observability" / "telemetry.py" in files
    assert {"redis", "sqlalchemy", "streamlit", "pandas", "plotly"} <= imports
    undeclared = _undeclared(imports, _declared_distributions(_demo_pin_lines()))
    assert not undeclared, f"undeclared hosted imports: {sorted(undeclared)}"


def test_demo_pin_gap_detector_fails_when_a_declared_dependency_is_removed():
    """Non-vacuity control: dropping the ``redis`` pin must be reported as a gap.

    Without this, the declaration test could pass for the wrong reason (an
    unfinished scan), which is exactly the failure mode it is meant to catch.
    """
    _, imports = _startup_third_party_imports()
    declared = _declared_distributions(_demo_pin_lines())
    assert not _undeclared(imports, declared)
    assert _undeclared(imports, {name for name in declared if name != "redis"}) == {"redis"}


def test_hosted_startup_needs_no_pipeline_stack_package():
    """The demo set stays minimal: no PySpark/Kafka/MinIO/PostgreSQL client import.

    A hosted import error is therefore fixed by declaring one missing package,
    never by widening the demo pins into the full pipeline stack.
    """
    _, imports = _startup_third_party_imports()
    declared = _declared_distributions(_demo_pin_lines())
    assert not imports & _PIPELINE_MODULES
    pipeline_distributions = {"pyspark", "confluent-kafka", "boto3", "minio", "psycopg2-binary",
                             "faker", "prometheus-client"}
    assert not declared & pipeline_distributions


def test_startup_scan_resolves_relative_and_ignores_deferred_imports(tmp_path, monkeypatch):
    """Adversarial control over the scanner itself, on a synthetic repository tree.

    A relative import must be followed into its sibling module, a
    function-level import must NOT be reported (it never runs at module load)
    and every discovered third-party name that is not declared must surface as
    a gap. Without this, the declaration test could pass while the scanner was
    blind to exactly the import styles the dashboard uses.
    """
    fake_root = tmp_path / "repo"
    (fake_root / "pkg").mkdir(parents=True)
    (fake_root / "pkg" / "__init__.py").write_text("", encoding="utf-8")
    (fake_root / "pkg" / "app.py").write_text(
        "import somepkg\nfrom . import sibling\n\n\ndef later():\n    import deferredpkg\n",
        encoding="utf-8",
    )
    (fake_root / "pkg" / "sibling.py").write_text("import other_thing\n", encoding="utf-8")
    entry = fake_root / "entry.py"
    entry.write_text("from pkg.app import x\n", encoding="utf-8")

    monkeypatch.setattr(sys.modules[__name__], "ROOT", fake_root)
    files, imports = _startup_third_party_imports(entry)

    assert fake_root / "pkg" / "sibling.py" in files
    assert imports == {"somepkg", "other_thing"}
    assert _undeclared(imports, {"somepkg"}) == {"other_thing"}
    assert not _undeclared(imports, {"somepkg", "other-thing"})
