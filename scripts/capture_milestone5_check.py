"""Append-only M5 evidence capture; credentials stay in the existing container env.

`outages` stops/starts only the OIP Redis and PostgreSQL containers, retaining all
volumes. Always restores each service, even when the verification command fails.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import time


def capture(command, output, label, timeout=900):
    started = datetime.now(timezone.utc).isoformat()
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    evidence = {"command": command, "returncode": result.returncode, "started_at": started,
                "finished_at": datetime.now(timezone.utc).isoformat()}
    log = result.stdout + "\n" + result.stderr
    for line in log.splitlines():
        if line.startswith("MILESTONE5_VERIFIED "):
            evidence["result"] = json.loads(line.split(" ", 1)[1])
    stem = output / f"{label}-{time.time_ns()}"
    stem.with_suffix(".json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    stem.with_suffix(".log").write_text(log, encoding="utf-8")
    print(f"{label}: exit={result.returncode}; evidence={stem.with_suffix('.json')}", flush=True)
    if "result" in evidence:
        data = evidence["result"]
        print(json.dumps({k: data[k] for k in ("postgres_version", "postgres_digest", "redis_digest", "rows",
                                              "key_count", "active_hash_fields", "redis_failure_reported",
                                              "postgres_failure_reported", "postgres_unchanged", "redis_unchanged")
                          if k in data}), flush=True)
    elif result.returncode:
        print(log, flush=True)
    else:
        print("\n".join(line for line in log.splitlines() if "passed" in line or "failed" in line), flush=True)
    if result.returncode:
        raise RuntimeError(f"{label} failed; see captured evidence")


def wait_healthy(docker, container):
    for _ in range(60):
        result = subprocess.run([docker, "inspect", "--format", "{{.State.Health.Status}}", container],
                                capture_output=True, text=True, check=True)
        if result.stdout.strip() == "healthy":
            return
        time.sleep(1)
    raise RuntimeError(f"{container} did not become healthy")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step", choices=["serve", "verify", "rebuild", "outages", "tests", "full-tests", "dependencies"],
                        required=True)
    parser.add_argument("--schema", default="gold_m4_final")
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()
    if not args.namespace.startswith("oip:m5:verify-"):
        parser.error("Runtime evidence must use an isolated oip:m5:verify- namespace")
    output = Path(__file__).resolve().parents[1] / "milestone5_evidence" / args.namespace.replace(":", "-")
    output.mkdir(parents=True, exist_ok=True)
    docker = shutil.which("docker.exe") or shutil.which("docker")
    base = [docker, "exec", "-e", "PYTHONPATH=/opt/oip", "-e", "REDIS_HOST=redis", "oip-modernization-verify"]

    def run(phase):
        command = base + ["python3", "-m", "scripts.verify_milestone5", "--schema", args.schema,
                          "--namespace", args.namespace, "--phase", phase]
        capture(command, output, phase)

    if args.step == "tests":
        capture(base + ["python3", "-m", "pytest", "-v", "--tb=short", "-p", "no:cacheprovider",
                        "/opt/oip/tests/unit/test_redis_serving.py"], output, args.step)
    elif args.step == "full-tests":
        capture(base + ["/opt/spark/bin/spark-submit", "/opt/oip/scripts/run_spark_tests.py", "tests/unit"],
                output, args.step)
    elif args.step == "dependencies":
        capture(base + ["python3", "-c", "import redis,sqlalchemy,psycopg2,sys; "
                        "print({'python':sys.version,'redis_py':redis.__version__,"
                        "'sqlalchemy':sqlalchemy.__version__,'psycopg2':psycopg2.__version__})"], output, "python-versions")
        capture([docker, "exec", "oip-redis", "redis-server", "--version"], output, "redis-version")
        capture([docker, "exec", "oip-postgres", "psql", "-U", "oip_user", "-d", "order_intelligence",
                 "-c", "SELECT version();"], output, "postgres-version")
    elif args.step == "outages":
        run("verify")
        for container, phase in (("oip-redis", "redis-down"), ("oip-postgres", "postgres-down")):
            try:
                capture([docker, "stop", container], output, "stop-" + container)
                run(phase)
            finally:
                capture([docker, "start", container], output, "start-" + container)
                wait_healthy(docker, container)
            run("serve")
    else:
        run(args.step)


if __name__ == "__main__":
    main()
