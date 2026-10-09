"""Bounded M7 capture; only OIP services are paused, with finally recovery."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import time

from scripts.capture_milestone5_check import wait_healthy


def capture(command, output, label, timeout=900):
    started = datetime.now(timezone.utc).isoformat()
    proc = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    log = proc.stdout + "\n" + proc.stderr
    evidence = {"command": command, "returncode": proc.returncode, "started_at": started,
                "finished_at": datetime.now(timezone.utc).isoformat()}
    for line in log.splitlines():
        if line.startswith("MILESTONE7_VERIFIED "):
            evidence["result"] = json.loads(line.split(" ", 1)[1])
    stem = output / f"{label}-{time.time_ns()}"
    stem.with_suffix(".json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    stem.with_suffix(".log").write_text(log, encoding="utf-8")
    print(f"{label}: exit={proc.returncode}; evidence={stem.with_suffix('.json')}", flush=True)
    if "result" in evidence:
        result = evidence["result"]
        print(json.dumps({"phase": result["phase"], "counts": result.get("result", {}).get("counts"),
                          "persisted_events": result.get("independent_history", {}).get("event_count"),
                          "failure_explicit": result.get("failure_explicit")}), flush=True)
    elif not proc.returncode:
        print("\n".join(line for line in log.splitlines() if "passed" in line or "failed" in line))
    if proc.returncode:
        print(log)
        raise RuntimeError(f"{label} failed: inspect evidence before continuing")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step", required=True, choices=["tests", "full-tests", "runtime", "checks", "cli"])
    parser.add_argument("--run", default="20261004a")
    args = parser.parse_args()
    if not args.run.isalnum():
        parser.error("Run label must be alphanumeric")
    output = Path(__file__).resolve().parents[1] / "milestone7_evidence" / args.run
    output.mkdir(parents=True, exist_ok=True)
    docker = shutil.which("docker.exe") or shutil.which("docker")
    base = [docker, "exec", "-e", "PYTHONPATH=/opt/oip", "-e", "REDIS_HOST=redis",
            "-e", "POSTGRES_GOLD_SCHEMA=gold_serving", "oip-modernization-verify"]

    def phase(name):
        return capture(base + ["python3", "-m", "scripts.verify_milestone7", "--phase", name, "--run", args.run], output, name)

    if args.step == "tests":
        capture(base + ["python3", "-m", "pytest", "-v", "--tb=short", "-p", "no:cacheprovider",
                        "/opt/oip/tests/unit/test_alerts.py"], output, "tests")
    elif args.step == "full-tests":
        capture(base + ["/opt/spark/bin/spark-submit", "/opt/oip/scripts/run_spark_tests.py", "tests/unit"], output, "full-tests")
    elif args.step == "checks":
        capture(base + ["python3", "-c", "import redis,sqlalchemy,psycopg2; import alerts.evaluator; "
                        "print({m.__name__:m.__version__ for m in [redis,sqlalchemy,psycopg2]})"], output, "imports-versions")
        capture([docker, "compose", "config", "--quiet"], output, "compose")
    elif args.step == "cli":
        capture(base + ["python3", "/opt/oip/main.py", "alerts", "--rules", "/opt/oip/alerts/rules.example.json"],
                output, "default-cli")
        capture(base + ["python3", "-m", "alerts.evaluator", "--rules", "/opt/oip/alerts/rules.example.json"],
                output, "default-cli-retry")
        capture(base + ["python3", "-c", "import json; from scripts.verify_milestone7 import independent_history; "
                        "from storage.postgres_redis_serving import get_postgres_engine; e=get_postgres_engine(); "
                        "r=independent_history(e,'alerts_serving'); assert r['event_count']==2 and r['evaluation_count']==4; "
                        "print('MILESTONE7_VERIFIED '+json.dumps({'phase':'default-cli-persistence',**r},default=str)); e.dispose()"],
                output, "default-persistence")
    else:
        baseline = phase("production")
        phase("restart")
        phase("controlled")
        try:
            capture([docker, "stop", "oip-redis"], output, "stop-redis")
            phase("redis-down")
        finally:
            capture([docker, "start", "oip-redis"], output, "start-redis")
            wait_healthy(docker, "oip-redis")
        try:
            capture([docker, "stop", "oip-postgres"], output, "stop-postgres")
            phase("postgres-down")
        finally:
            capture([docker, "start", "oip-postgres"], output, "start-postgres")
            wait_healthy(docker, "oip-postgres")
        after = phase("production")
        assert baseline["result"]["independent_history"] == after["result"]["independent_history"]
        phase("restart")


if __name__ == "__main__":
    main()
