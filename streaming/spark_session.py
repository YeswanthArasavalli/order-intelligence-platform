"""
Order Intelligence Platform — Phase 4
Spark Session Factory
=====================
Creates and returns a configured SparkSession for structured streaming.
Includes S3A connector (MinIO), Kafka connector, and Avro support.

Usage:
    from streaming.spark_session import get_spark_session
    spark = get_spark_session()
"""

import os

from pyspark.sql import SparkSession

from utils.logger import configure_logging, get_logger

configure_logging()
log = get_logger("spark_session")

# ---------------------------------------------------------------------------
# Package versions — must align with pyspark version in requirements.txt
# PySpark 3.5.9 → Spark 3.5.9
# ---------------------------------------------------------------------------

SPARK_VERSION    = "3.5.9"
SCALA_VERSION    = "2.12"
KAFKA_PKG        = f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"
AVRO_PKG         = f"org.apache.spark:spark-avro_{SCALA_VERSION}:{SPARK_VERSION}"
HADOOP_AWS_PKG   = "org.apache.hadoop:hadoop-aws:3.3.4"
# Match Hadoop 3.3.4's published POM; do not independently upgrade the SDK.
AWS_SDK_PKG      = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
POSTGRES_PKG     = "org.postgresql:postgresql:42.7.3"


def get_spark_session(app_name: str = "OrderIntelligencePlatform") -> SparkSession:
    """
    Build and return a SparkSession configured for:
      - Kafka structured streaming (KRaft-compatible — no Zookeeper needed)
      - Avro deserialization via Schema Registry
      - MinIO / S3A for Bronze/Silver/Gold Parquet storage
      - PostgreSQL JDBC for Gold layer writes

    The session is a singleton — calling get_spark_session() multiple times
    returns the same session.
    """
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user     = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    spark_master   = os.getenv("SPARK_MASTER", "local[*]")

    log.info("spark_session_initialising",
             app_name=app_name,
             master=spark_master,
             minio_endpoint=minio_endpoint)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)

        # ── Packages (downloaded once, cached in ~/.ivy2) ──────────────────
        .config("spark.jars.packages",
                ",".join([KAFKA_PKG, AVRO_PKG, HADOOP_AWS_PKG, AWS_SDK_PKG, POSTGRES_PKG]))

        # ── S3A / MinIO configuration ──────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",              minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key",            minio_user)
        .config("spark.hadoop.fs.s3a.secret.key",            minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # ── Streaming behaviour ────────────────────────────────────────────
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.checkpointLocation",
                os.getenv("SPARK_CHECKPOINT_DIR", "s3a://checkpoints/streaming/"))

        # ── Shuffle / performance (dev-friendly settings) ──────────────────
        .config("spark.sql.shuffle.partitions",  "8")
        .config("spark.default.parallelism",     "8")
        .config("spark.sql.adaptive.enabled",    "true")

        # ── Logging ────────────────────────────────────────────────────────
        .config("spark.eventLog.enabled",        "false")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    log.info("spark_session_ready",
             spark_version=spark.version,
             app_id=spark.sparkContext.applicationId)

    return spark
