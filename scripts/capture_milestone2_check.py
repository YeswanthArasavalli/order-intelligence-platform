"""Run one Docker Spark check and preserve its full output plus JSON evidence.

Host usage: python3 scripts/capture_milestone2_check.py --step initial --topic
order-events-m2-<unique-run>. Other steps: restart, tests, regression.
"""

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9",
    "org.apache.spark:spark-avro_2.12:3.5.9",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.7.3",
])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step", choices=["initial", "restart", "recovery", "legacy", "tests", "regression"], required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--expected-count", type=int)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = root / "milestone2_evidence" / args.topic
    output.mkdir(parents=True, exist_ok=True)
    docker = shutil.which("docker.exe") or shutil.which("docker")
    command = [docker, "exec"]
    if args.step == "regression":
        assert args.expected_count and args.expected_count > 0
        command += ["-e", f"KAFKA_TOPIC_ORDERS={args.topic}", "-e",
                    f"SPARK_CHECKPOINT_DIR=s3a://checkpoints/{args.topic}"]
    command += ["oip-modernization-verify", "/opt/spark/bin/spark-submit", "--packages", PACKAGES,
                "--conf", "spark.jars.ivy=/tmp/.ivy2"]
    if args.step == "tests":
        command += ["/opt/oip/scripts/run_spark_tests.py"]
    elif args.step == "regression":
        command += ["/opt/oip/scripts/verify_milestone1_bronze.py", "--expected-count", str(args.expected_count)]
    else:
        command += ["/opt/oip/scripts/verify_milestone2.py", "--topic", args.topic, "--phase", args.step]
    result = subprocess.run(command, capture_output=True, text=True, timeout=600)
    text = result.stdout + "\n" + result.stderr
    stem = output / args.step
    stem.with_suffix(".log").write_text(text, encoding="utf-8")
    evidence = {"command": command, "returncode": result.returncode}
    for line in text.splitlines():
        if line.startswith(("MILESTONE2_VERIFIED ", "MILESTONE1_VERIFIED ")):
            evidence["result"] = json.loads(line.split(" ", 1)[1])
    stem.with_suffix(".json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(f"{args.step}: exit={result.returncode}; evidence={stem.with_suffix('.json')}")
    if result.returncode:
        print(text)
    elif "result" in evidence:
        print(json.dumps({k: v for k, v in evidence["result"].items()
                          if k in {"topic", "phase", "bronze_rows", "silver_rows", "dlq_rows", "audit_rows",
                                   "restart_no_input", "fresh_checkpoint_replay_rows", "count", "schema"}}, indent=2))
    else:
        print("\n".join(line for line in text.splitlines() if "passed" in line or "FAILED" in line))
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
