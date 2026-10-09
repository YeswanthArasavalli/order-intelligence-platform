"""Append-only M9 capture; excludes credentials from command/result construction."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time


def capture(command, output, label, timeout=1200, allowed=(0,)):
    started = datetime.now(timezone.utc).isoformat()
    proc = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    log = proc.stdout + "\n" + proc.stderr
    evidence = {"command": command, "returncode": proc.returncode, "started_at": started,
                "finished_at": datetime.now(timezone.utc).isoformat()}
    for line in log.splitlines():
        if line.startswith("MILESTONE9_VERIFIED "):
            evidence["result"] = json.loads(line.split(" ", 1)[1])
    output.mkdir(parents=True, exist_ok=True)
    stem = output / f"{label}-{time.time_ns()}"
    stem.with_suffix(".json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    stem.with_suffix(".log").write_text(log, encoding="utf-8")
    print(f"{label}: exit={proc.returncode}; evidence={stem.with_suffix('.json')}", flush=True)
    print("\n".join(line for line in log.splitlines() if len(line) < 500 and ("passed" in line or "FAILED" in line)), flush=True)
    if proc.returncode not in allowed:
        print(log, flush=True)
        raise RuntimeError(f"{label} failed; investigate retained evidence")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step", required=True)
    parser.add_argument("--run", default="20261004a")
    args = parser.parse_args()
    if not args.run.isalnum():
        parser.error("Run must be alphanumeric")
    output = Path(__file__).resolve().parents[1] / "milestone9_evidence" / args.run
    docker = shutil.which("docker.exe") or shutil.which("docker")
    base = [docker, "exec", "-e", "PYTHONPATH=/opt/oip", "-e", "REDIS_HOST=redis", "oip-modernization-verify"]
    instrumented = base[:-1] + ["-e", "OIP_TELEMETRY_URL=http://metrics-exporter:8000/observe", base[-1]]
    if args.step == "baseline":
        capture(base + ["python3", "-m", "scripts.milestone9_baseline"], output, args.step)
    elif args.step in {"tests", "full-tests"}:
        command = ["/opt/spark/bin/spark-submit", "/opt/oip/scripts/run_spark_tests.py",
                   "tests/unit/test_observability.py" if args.step == "tests" else "tests/unit"]
        capture(base + command, output, args.step)
    elif args.step in {"stack", "activity", "target-down", "restart", "postgres-down", "redis-down", "application-restart"}:
        python = str(Path(__file__).resolve().parents[1] / ".venv" / "Scripts" / "python.exe")
        capture([python, "-m", "scripts.verify_milestone9", "--phase", args.step], output, args.step)
    elif args.step in {"serving", "alerts", "notifications", "no-data", "independence", "observe-postgres-down", "observe-redis-down"}:
        name = args.step.removeprefix("observe-")
        runner = (["/opt/spark/bin/spark-submit", "/opt/oip/scripts/milestone9_activity.py"]
                  if name == "no-data" else ["python3", "-m", "scripts.milestone9_activity"])
        capture(instrumented + runner + ["--phase", name, "--run", args.run], output, args.step)
    elif args.step == "pipeline":
        from scripts.capture_milestone2_check import PACKAGES
        spark = instrumented + ["/opt/spark/bin/spark-submit", "--packages", PACKAGES, "--conf", "spark.jars.ivy=/tmp/.ivy2"]
        capture(spark + ["/opt/oip/scripts/verify_milestone3_m1.py", "--topic", "order-events-m1-m9observe-" + args.run], output, "pipeline-m1")
        capture(spark + ["/opt/oip/scripts/verify_milestone2.py", "--phase", "initial", "--topic", "order-events-m2-m9observe-" + args.run], output, "pipeline-m2")
        capture(spark + ["/opt/oip/scripts/verify_milestone3.py", "--phase", "initial", "--topic", "order-events-m3-m9observe-" + args.run], output, "pipeline-m3")
    elif args.step == "cooldown":
        capture(instrumented + ["python3", "-m", "scripts.verify_milestone7", "--phase", "controlled", "--run", "m9obs" + args.run], output, "controlled-m7")
    elif args.step == "failures":
        from scripts.capture_milestone5_check import wait_healthy
        python = str(Path(__file__).resolve().parents[1] / ".venv" / "Scripts" / "python.exe")

        def api(phase):
            capture([python, "-m", "scripts.verify_milestone9", "--phase", phase], output, "failure-" + phase)

        def activity(phase):
            capture(instrumented + ["python3", "-m", "scripts.milestone9_activity", "--phase", phase, "--run", args.run], output, "failure-activity-" + phase)

        for container, phase in (("oip-postgres", "postgres-down"), ("oip-redis", "redis-down")):
            try:
                capture([docker, "stop", container], output, "stop-" + container)
                activity(phase)
                api(phase)
            finally:
                capture([docker, "start", container], output, "start-" + container)
                wait_healthy(docker, container)
            api("stack")
        try:
            capture([docker, "stop", "oip-metrics-exporter"], output, "stop-exporter")
            api("target-down")
            activity("independence")
        finally:
            capture([docker, "start", "oip-metrics-exporter"], output, "start-exporter")
            wait_healthy(docker, "oip-metrics-exporter")
        api("restart")
        for container in ("oip-prometheus", "oip-grafana"):
            try:
                capture([docker, "stop", container], output, "stop-" + container)
                activity("independence")
                from scripts.capture_milestone2_check import PACKAGES
                phase = "second" if container == "oip-prometheus" else "third"
                capture(instrumented + ["/opt/spark/bin/spark-submit", "--packages", PACKAGES,
                    "--conf", "spark.jars.ivy=/tmp/.ivy2", "/opt/oip/scripts/verify_milestone3.py", "--phase", phase,
                    "--topic", "order-events-m3-m9observe-" + args.run], output, "pipeline-with-" + container + "-down")
            finally:
                capture([docker, "start", container], output, "start-" + container)
                wait_healthy(docker, container)
            api("stack")
    elif args.step == "regression-early":
        jobs = [("m1", "order-events-m1-m9-" + args.run),
                ("m2-initial", "order-events-m2-m9-" + args.run),
                ("m2-restart", "order-events-m2-m9-" + args.run),
                ("m2-recovery", "order-events-m2-m9-" + args.run)]
        jobs += [(phase, "order-events-m3-m9-" + args.run) for phase in ("initial", "second", "third", "replay", "recovery")]
        for step, topic in jobs:
            capture([sys.executable, "-m", "scripts.capture_milestone3_check", "--step", step, "--topic", topic],
                    output, "regression-" + step)
    elif args.step == "regression-serving":
        topic = "order-events-m3-m9-" + args.run
        for phase in ("initial", "retry", "failure", "old", "corrupt"):
            capture([sys.executable, "-m", "scripts.capture_milestone4_check", "--phase", phase, "--topic", topic,
                     "--schema", "gold_m4_m9reg" + args.run], output, "regression-m4-" + phase)
        for step in ("serve", "serve", "rebuild", "outages"):
            capture([sys.executable, "-m", "scripts.capture_milestone5_check", "--step", step, "--schema", "gold_serving",
                     "--namespace", "oip:m5:verify-m9reg-" + args.run], output, "regression-m5-" + step)
        for step in ("production", "runtime"):
            capture([sys.executable, "-m", "scripts.capture_milestone6_check", "--step", step, "--run", "m9" + args.run],
                    output, "regression-m6-" + step)
        capture([sys.executable, "-m", "scripts.capture_milestone7_check", "--step", "runtime", "--run", "m9" + args.run],
                output, "regression-m7")
    elif args.step == "regression-m8":
        # A fresh database avoids changing retained M8 terminal requests/receiver receipts.
        # The frozen verifier expects a fresh controlled scenario, not replay of terminal work.
        from scripts.capture_milestone8_check import capture as capture8
        from scripts.capture_milestone5_check import wait_healthy
        database = "m9_m8_" + args.run
        capture([docker, "exec", "oip-postgres", "psql", "-U", "oip_user", "-d", "order_intelligence", "-v", "ON_ERROR_STOP=1",
                 "-c", f'CREATE DATABASE "{database}"'], output, "m8-create-isolated-database")
        dump = subprocess.run([docker, "exec", "oip-postgres", "pg_dump", "-U", "oip_user", "-d", "order_intelligence",
                               "--schema=alerts_serving", "--no-owner", "--no-privileges"], capture_output=True, text=True, check=True)
        restore = subprocess.run([docker, "exec", "-i", "oip-postgres", "psql", "-U", "oip_user", "-d", database,
                                  "-v", "ON_ERROR_STOP=1"], input=dump.stdout, capture_output=True, text=True, check=True)
        print("Isolated M7 event copy restored; psql exit", restore.returncode, flush=True)
        run = "m9" + args.run
        target = output.parents[1] / "milestone8_evidence" / run
        target.mkdir(parents=True, exist_ok=True)
        command = instrumented[:-1] + ["-e", "POSTGRES_DB=" + database, instrumented[-1], "python3", "-m", "scripts.verify_milestone8"]
        for phase in ("inspect", "runtime"):
            capture8(command + ["--phase", phase, "--run", run], target, phase)
        for container, phase in (("oip-redis", "redis-down"), ("oip-postgres", "postgres-down")):
            try:
                capture([docker, "stop", container], output, "m8-stop-" + container)
                capture8(command + ["--phase", phase, "--run", run], target, phase)
            finally:
                capture([docker, "start", container], output, "m8-start-" + container)
                wait_healthy(docker, container)
            capture8(command + ["--phase", "recovery", "--run", run], target, "recovery")
        capture8(command + ["--phase", "integrity", "--run", run], target, "integrity")
    elif args.step == "audit":
        code = '''import json
from pathlib import Path
from dataclasses import asdict
from scripts.milestone9_baseline import state
from observability.catalog import SPECS, Metrics
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families
root=Path('/opt/oip')
before=json.loads(next((root/'milestone9_evidence'/'20261004a').glob('baseline-*.json')).read_text())['result']
after=state()
changed=[]
assert set(before['files']) == set(after['files'])
for path, record in before['files'].items():
    current=after['files'][path]
    if record['sha256'] != current['sha256']:
        assert record.get('business_ast') and record['business_ast'] == current.get('business_ast'), path
        changed.append(path)
assert {k:v for k,v in before.items() if k!='files'} == {k:v for k,v in after.items() if k!='files'}
metrics=Metrics()
samples=sum(len(f.samples) for f in text_string_to_metric_families(generate_latest(metrics.registry).decode()))
assert samples < 3000
print('MILESTONE9_VERIFIED '+json.dumps({'business_data_unchanged':True,'business_ast_unchanged':True,
    'instrumented_files':changed,'byte_unchanged_files':len(before['files'])-len(changed),
    'business_state':{k:v for k,v in after.items() if k!='files'},'metric_family_count':len(SPECS),
    'bounded_exposition_samples':samples,'metrics':{k:asdict(v) for k,v in SPECS.items()}},sort_keys=True))'''
        capture(base + ["python3", "-c", code], output, "audit")
    elif args.step == "freeze":
        containers = ["oip-modernization-verify", "oip-metrics-exporter", "oip-prometheus", "oip-grafana",
                      "oip-schema-registry", "oip-kafka", "oip-minio", "oip-redis", "oip-postgres"]
        capture([docker, "ps", "--format", "{{.Names}} {{.Status}}"], output, "freeze-before")
        capture([docker, "stop", *containers], output, "freeze-stop", timeout=180)
        capture([docker, "inspect", "--format", "{{.Name}} {{.State.Status}} {{.State.Running}}", *containers],
                output, "freeze-state")
        capture([docker, "ps", "--format", "{{.Names}} {{.Status}}"], output, "freeze-after")
    elif args.step == "checks":
        python = str(Path(__file__).resolve().parents[1] / ".venv" / "Scripts" / "python.exe")
        files = ["observability", "scripts/capture_milestone9_check.py", "scripts/milestone9_baseline.py",
                 "scripts/milestone9_activity.py", "scripts/verify_milestone9.py", "tests/unit/test_observability.py"]
        capture([python, "-m", "ruff", "check", *files], output, "ruff")
        capture([python, "-m", "compileall", "-q", *files, "producer", "streaming", "storage", "dashboard", "alerts", "notifications"], output, "compile")
        capture([python, "-m", "pip", "check"], output, "host-pip")
        capture(base + ["python3", "-m", "pip", "check"], output, "verification-pip", allowed=(0,1))
        capture([docker, "exec", "oip-metrics-exporter", "python", "-m", "pip", "check"], output, "exporter-pip")
        capture([docker, "compose", "config", "--quiet"], output, "compose-final")
        capture([docker, "exec", "oip-prometheus", "promtool", "check", "config", "/etc/prometheus/prometheus.yml"], output, "promtool")
    else:
        raise ValueError("Phase not yet implemented")


if __name__ == "__main__":
    main()
