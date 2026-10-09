"""Capture real M4 Spark/PostgreSQL serving commands and JSON evidence."""

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import time

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9",
    "org.apache.spark:spark-avro_2.12:3.5.9",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.7.3",
])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["initial", "retry", "failure", "old", "corrupt"], required=True)
    parser.add_argument("--topic", default="order-events-m3-20261003b")
    parser.add_argument("--schema", default="gold_m4_runtime")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = root / "milestone4_evidence" / args.topic
    output.mkdir(parents=True, exist_ok=True)
    docker = shutil.which("docker.exe") or shutil.which("docker")
    command = [docker, "exec", "-e", "PYTHONPATH=/opt/oip", "-e", f"POSTGRES_GOLD_SCHEMA={args.schema}",
               "oip-modernization-verify",
               "/opt/spark/bin/spark-submit", "--packages", PACKAGES,
               "--conf", "spark.jars.ivy=/tmp/.ivy2", "/opt/oip/scripts/verify_milestone4.py",
               "--phase", args.phase, "--topic", args.topic, "--schema", args.schema]
    result = subprocess.run(command, capture_output=True, text=True, timeout=900)
    output.mkdir(parents=True, exist_ok=True)
    text = result.stdout + "\n" + result.stderr
    stem = output / f"{args.phase}-{time.time_ns()}"
    stem.with_suffix(".log").write_text(text, encoding="utf-8")
    evidence = {"command": command, "returncode": result.returncode}
    for line in text.splitlines():
        if line.startswith("MILESTONE4_VERIFIED "):
            evidence["result"] = json.loads(line.split(" ", 1)[1])
    stem.with_suffix(".json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(f"{args.phase}: exit={result.returncode}; evidence={stem.with_suffix('.json')}")
    if result.returncode:
        print(text)
    elif "result" in evidence:
        print(json.dumps({k: v for k, v in evidence["result"].items() if k in {
            "phase", "gold_version", "manifest_path", "snapshot_digest", "gold_rows", "postgres_rows",
            "same_manifest_noop", "failure_before_commit_preserved", "old_manifest_rejected",
            "uncommitted_status", "corrupt_manifest_rejected"}}, indent=2))
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
