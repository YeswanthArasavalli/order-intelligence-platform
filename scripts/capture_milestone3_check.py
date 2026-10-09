"""Capture bounded Spark checks with append-only local evidence filenames."""

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

from scripts.capture_milestone2_check import PACKAGES


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step", required=True, choices=["initial", "second", "third", "replay", "recovery",
                                                        "tests", "m1", "m2-initial", "m2-restart", "m2-recovery"])
    parser.add_argument("--topic", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = root / "milestone3_evidence" / args.topic
    output.mkdir(parents=True, exist_ok=True)
    command = [shutil.which("docker.exe") or shutil.which("docker"), "exec", "oip-modernization-verify",
               "/opt/spark/bin/spark-submit", "--packages", PACKAGES, "--conf", "spark.jars.ivy=/tmp/.ivy2"]
    if args.step == "tests":
        command += ["/opt/oip/scripts/run_spark_tests.py", "tests/unit"]
    elif args.step.startswith("m2-"):
        command += ["/opt/oip/scripts/verify_milestone2.py", "--topic", args.topic, "--phase", args.step[3:]]
    elif args.step == "m1":
        command += ["/opt/oip/scripts/verify_milestone3_m1.py", "--topic", args.topic]
    else:
        command += ["/opt/oip/scripts/verify_milestone3.py", "--topic", args.topic, "--phase", args.step]
    result = subprocess.run(command, capture_output=True, text=True, timeout=900)
    text = result.stdout + "\n" + result.stderr
    stem = output / f"{args.step}-{time.time_ns()}"
    stem.with_suffix(".log").write_text(text, encoding="utf-8")
    evidence = {"command": command, "returncode": result.returncode}
    for line in text.splitlines():
        if line.startswith(("MILESTONE3_VERIFIED ", "MILESTONE2_VERIFIED ", "MILESTONE1_VERIFIED ", "M1_PRODUCER_VERIFIED ")):
            evidence[line.split(" ", 1)[0]] = json.loads(line.split(" ", 1)[1])
    stem.with_suffix(".json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(f"{args.step}: exit={result.returncode}; evidence={stem.with_suffix('.json')}")
    if result.returncode:
        print(text)
    else:
        for name, data in evidence.items():
            if isinstance(data, dict):
                print(name, json.dumps({k: v for k, v in data.items() if k in {
                    "phase", "topic", "bronze_count", "silver_count", "gold_events", "gold_rows", "dlq_count",
                    "logical_digest", "count", "bronze_rows", "silver_rows", "dlq_rows", "audit_rows"}}))
        print("\n".join(line for line in text.splitlines() if " passed" in line or "FAILED" in line))
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
