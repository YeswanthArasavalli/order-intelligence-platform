"""Drain a dedicated smoke topic, verify Bronze, and exit without deleting data.

Run with spark-submit and the packages in streaming.spark_session.
Set KAFKA_TOPIC_ORDERS and SPARK_CHECKPOINT_DIR to dedicated smoke locations.
--expected-count is the cumulative Bronze row count, including prior runs.
"""

import argparse
import json
import os
import platform
from functools import reduce

from pyspark.sql import functions as F

from streaming.bronze_writer import bronze_path, read_kafka_stream, write_to_bronze
from streaming.spark_session import SPARK_VERSION, get_spark_session


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-count", type=int, required=True)
    args = parser.parse_args()
    if args.expected_count <= 0:
        parser.error("--expected-count must be positive")

    topic = os.environ["KAFKA_TOPIC_ORDERS"]
    checkpoint = os.environ["SPARK_CHECKPOINT_DIR"].rstrip("/") + "/bronze/"
    path = bronze_path(os.getenv("MINIO_BRONZE_BUCKET", "bronze"), topic)
    spark = get_spark_session("OIP-Milestone1Verification")
    query = None
    try:
        assert spark.version == SPARK_VERSION
        assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
        stream = read_kafka_stream(spark, topic, os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        query = (
            stream.writeStream.foreachBatch(write_to_bronze)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        )
        if not query.awaitTermination(120):
            raise TimeoutError("Bronze did not drain within 120 seconds")

        bronze = spark.read.parquet(path)
        required = [
            "order_id", "event_type", "amount", "timestamp", "event_ts",
            "_kafka_offset", "_kafka_partition", "region",
        ]
        assert set(required) <= set(bronze.columns)
        count = bronze.count()
        assert count == args.expected_count, (count, args.expected_count)
        nulls = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in required])
        assert bronze.filter(nulls).count() == 0
        assert bronze.select("_kafka_partition", "_kafka_offset").distinct().count() == count
        assert bronze.filter(
            F.expr("unix_millis(event_ts) != timestamp")
        ).count() == 0

        jvm = spark.sparkContext._jvm
        location = jvm.org.apache.hadoop.fs.Path(checkpoint)
        fs = location.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        for suffix in ("metadata", "offsets/0", "sources/0/0", "commits/0"):
            assert fs.exists(jvm.org.apache.hadoop.fs.Path(checkpoint + suffix)), suffix

        print("MILESTONE1_VERIFIED", json.dumps({
            "spark": spark.version,
            "python": platform.python_version(),
            "java": jvm.java.lang.System.getProperty("java.version"),
            "hadoop": jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
            "aws_sdk": jvm.com.amazonaws.util.VersionInfoUtils.getVersion(),
            "topic": topic,
            "bronze_path": path,
            "count": count,
            "checkpoint": checkpoint,
            "input_rows_this_run": sum(p["numInputRows"] for p in query.recentProgress),
            "schema": bronze.schema.simpleString(),
        }))
        bronze.select(*required).orderBy("_kafka_partition", "_kafka_offset").show(
            3, truncate=False
        )
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
