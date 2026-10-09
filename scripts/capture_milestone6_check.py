"""Append-only M6 evidence; secrets remain in the retained container environment."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import time

from scripts.capture_milestone4_check import PACKAGES
from scripts.capture_milestone5_check import wait_healthy


def capture(command, output, label, timeout=900):
    started = datetime.now(timezone.utc).isoformat()
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    log = result.stdout + "\n" + result.stderr
    evidence = {"command": command, "returncode": result.returncode, "started_at": started,
                "finished_at": datetime.now(timezone.utc).isoformat()}
    for line in log.splitlines():
        if line.startswith("MILESTONE6_VERIFIED "):
            evidence["result"] = json.loads(line.split(" ", 1)[1])
    stem = output / f"{label}-{time.time_ns()}"
    stem.with_suffix(".json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    stem.with_suffix(".log").write_text(log, encoding="utf-8")
    print(f"{label}: exit={result.returncode}; evidence={stem.with_suffix('.json')}", flush=True)
    if "result" in evidence:
        data = evidence["result"]
        print(json.dumps({key: data[key] for key in ("phase", "schema", "namespace", "dashboard_digest", "rows") if key in data}))
    elif not result.returncode:
        print("\n".join(line for line in log.splitlines() if "passed" in line or "failed" in line))
    if result.returncode:
        print(log)
        raise RuntimeError(f"{label} failed; stop and investigate")
    return evidence.get("result")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step", choices=["qualify", "tests", "full-tests", "production", "runtime", "checks"], required=True)
    parser.add_argument("--run", default="20261004a")
    args = parser.parse_args()
    if not args.run.isalnum():
        parser.error("Run label must be alphanumeric")
    output = Path(__file__).resolve().parents[1] / "milestone6_evidence" / args.run
    output.mkdir(parents=True, exist_ok=True)
    docker = shutil.which("docker.exe") or shutil.which("docker")
    base = [docker, "exec", "-e", "PYTHONPATH=/opt/oip", "-e", "REDIS_HOST=redis",
            "-e", "POSTGRES_GOLD_SCHEMA=gold_serving", "oip-modernization-verify"]
    ns = "oip:m5:verify-m6-" + args.run

    def run(phase, namespace=ns, digest=None):
        command = base + ["python3", "-m", "scripts.verify_milestone6", "--phase", phase, "--namespace", namespace]
        if digest:
            command += ["--expected-digest", digest]
        return capture(command, output, phase)

    if args.step == "qualify":
        capture(base + ["/opt/spark/bin/spark-submit", "--packages", PACKAGES, "--conf", "spark.jars.ivy=/tmp/.ivy2",
                        "/opt/oip/scripts/qualify_m6_source.py"], output, "qualify")
    elif args.step == "tests":
        capture(base + ["python3", "-m", "pytest", "-v", "--tb=short", "-p", "no:cacheprovider",
                        "/opt/oip/tests/unit/test_dashboard.py"], output, "tests")
    elif args.step == "full-tests":
        capture(base + ["/opt/spark/bin/spark-submit", "/opt/oip/scripts/run_spark_tests.py", "tests/unit"], output, "full-tests")
    elif args.step == "production":
        run("production", "oip:m5:gold-serving")
    elif args.step == "checks":
        capture(base + ["python3", "-c", "import streamlit,pandas,plotly,redis,sqlalchemy,psycopg2; "
                        "print({m.__name__:m.__version__ for m in [streamlit,pandas,plotly,redis,sqlalchemy,psycopg2]})"],
                output, "versions")
        capture([docker, "compose", "config", "--quiet"], output, "compose")
    else:
        baseline = run("healthy")
        run("missing")
        run("rebuild")
        run("corrupt")
        try:
            capture([docker, "stop", "oip-redis"], output, "stop-redis")
            run("redis-down")
        finally:
            capture([docker, "start", "oip-redis"], output, "start-redis")
            wait_healthy(docker, "oip-redis")
        run("healthy")
        try:
            capture([docker, "stop", "oip-postgres"], output, "stop-postgres")
            run("postgres-down", digest=baseline["dashboard_digest"])
            capture([docker, "stop", "oip-redis"], output, "stop-both")
            run("both-down")
        finally:
            capture([docker, "start", "oip-postgres", "oip-redis"], output, "restore-both")
            wait_healthy(docker, "oip-postgres")
            wait_healthy(docker, "oip-redis")
        run("healthy")


if __name__ == "__main__":
    main()
