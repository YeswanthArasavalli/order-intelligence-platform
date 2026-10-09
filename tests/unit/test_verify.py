"""Tests for the self-contained verification dispatcher (scripts/verify.py)."""

import importlib
from pathlib import Path

import pytest

import scripts.verify as verify

ROOT = Path(__file__).resolve().parents[2]


def test_declared_suites_exist_as_files():
    for relative in (*verify.SELF_CONTAINED_SUITES, *verify.SERVICE_DEPENDENT_SUITES):
        assert (ROOT / relative).is_file(), relative


def test_suite_declares_are_disjoint_and_cover_every_unit_file():
    self_contained = {Path(p).name for p in verify.SELF_CONTAINED_SUITES}
    service = {Path(p).name for p in verify.SERVICE_DEPENDENT_SUITES}
    collected = {p.name for p in (ROOT / "tests" / "unit").glob("test_*.py")}
    assert not (self_contained & service)
    assert self_contained | service == collected


def test_suite_paths_resolve_and_missing_files_fail_fast(tmp_path, monkeypatch):
    (tmp_path / "a.py").write_text("")
    monkeypatch.setattr(verify, "ROOT", tmp_path)
    assert verify.suite_paths(("a.py",)) == [str(tmp_path / "a.py")]
    with pytest.raises(FileNotFoundError):
        verify.suite_paths(("missing.py",))


def test_main_dispatcher_entry_resolves():
    module = importlib.import_module("scripts.verify")
    assert callable(module.run)


def test_run_invokes_pytest_only_on_self_contained_suites(monkeypatch):
    seen = {}

    def fake_main(args, **kwargs):
        seen["args"] = list(args)
        return 0

    monkeypatch.setattr(verify.pytest, "main", fake_main)
    with pytest.raises(SystemExit) as exit_info:
        verify.run()
    assert exit_info.value.code == 0
    targets = [arg for arg in seen["args"] if not arg.startswith("-")]
    assert targets == list(verify.suite_paths(verify.SELF_CONTAINED_SUITES))
    for relative in verify.SERVICE_DEPENDENT_SUITES:
        assert not any(arg.endswith(relative) for arg in targets)
