"""Append-only M8 evidence capture; credentials remain in existing process environment."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import time

from scripts.capture_milestone5_check import wait_healthy


def capture(command, output, label, timeout=900, allowed=(0,)):
    started = datetime.now(timezone.utc).isoformat()
    proc = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    log = proc.stdout + "\n" + proc.stderr
    evidence = {"command": command, "returncode": proc.returncode, "started_at": started,
                "finished_at": datetime.now(timezone.utc).isoformat()}
    for line in log.splitlines():
        if line.startswith("MILESTONE8_VERIFIED "):
            evidence["result"] = json.loads(line.split(" ", 1)[1])
    stem = output / f"{label}-{time.time_ns()}"
    stem.with_suffix(".json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    stem.with_suffix(".log").write_text(log, encoding="utf-8")
    print(f"{label}: exit={proc.returncode}; evidence={stem.with_suffix('.json')}", flush=True)
    if "result" in evidence:
        result = evidence["result"]
        print(json.dumps({k: result[k] for k in ("phase", "m7_source_unchanged", "postgres_failure_explicit",
                         "redis_independent", "external_provider_certified", "frozen_file_count") if k in result}), flush=True)
    else:
        print("\n".join(line for line in log.splitlines() if "passed" in line or "failed" in line), flush=True)
    if proc.returncode not in allowed:
        print(log, flush=True)
        raise RuntimeError(f"{label} failed; retained evidence must be investigated")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step", required=True, choices=["inspect", "tests", "full-tests", "runtime", "outages", "checks", "freeze"])
    parser.add_argument("--run", default="20261004a")
    args = parser.parse_args()
    if not args.run.isalnum():
        parser.error("Run must be alphanumeric")
    root = Path(__file__).resolve().parents[1]
    output = root / "milestone8_evidence" / args.run
    output.mkdir(parents=True, exist_ok=True)
    docker = shutil.which("docker.exe") or shutil.which("docker")
    base = [docker, "exec", "-e", "PYTHONPATH=/opt/oip", "-e", "REDIS_HOST=redis",
            "-e", "POSTGRES_GOLD_SCHEMA=gold_serving", "oip-modernization-verify"]

    def phase(name):
        return capture(base + ["python3", "-m", "scripts.verify_milestone8", "--phase", name, "--run", args.run], output, name)

    if args.step == "inspect":
        phase("inspect")
    elif args.step == "tests":
        capture(base + ["python3", "-m", "pytest", "-v", "--tb=short", "-p", "no:cacheprovider",
                        "/opt/oip/tests/unit/test_notifications.py"], output, "tests")
    elif args.step == "full-tests":
        capture(base + ["/opt/spark/bin/spark-submit", "/opt/oip/scripts/run_spark_tests.py", "tests/unit"], output, "full-tests")
    elif args.step == "runtime":
        phase("runtime")
    elif args.step == "outages":
        for name, result in (("oip-redis", "redis-down"), ("oip-postgres", "postgres-down")):
            try:
                capture([docker, "stop", name], output, "stop-" + name)
                phase(result)
            finally:
                capture([docker, "start", name], output, "start-" + name)
                wait_healthy(docker, name)
            phase("recovery")
    elif args.step == "freeze":
        names = ["oip-modernization-verify", "oip-schema-registry", "oip-kafka", "oip-minio", "oip-redis", "oip-postgres"]
        capture([docker, "stop", *names], output, "freeze-stop")
        capture([docker, "inspect", "--format", "{{.Name}} {{.State.Status}} {{.State.ExitCode}}", *names], output, "freeze-status")
    else:
        files = ["notifications", "scripts/milestone8_receiver.py", "scripts/verify_milestone8.py",
                 "scripts/capture_milestone8_check.py", "tests/unit/test_notifications.py", "main.py"]
        python = str(root / ".venv" / "Scripts" / "python.exe")
        capture([python, "-m", "ruff", "check", *files], output, "ruff")
        capture([python, "-m", "compileall", "-q", *files], output, "compile")
        capture([python, "-m", "pip", "check"], output, "host-pip")
        capture(base + ["python3", "-m", "pip", "check"], output, "container-pip", allowed=(0, 1))
        capture([docker, "compose", "config", "--quiet"], output, "compose")
        phase("integrity")


if __name__ == "__main__":
    main()
