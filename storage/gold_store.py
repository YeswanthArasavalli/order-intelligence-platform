"""Immutable Parquet snapshots, atomically exposed by a single MinIO manifest.

One writer per root. Readers use only manifest-pinned paths, never snapshot globs.
S3A closes a single object atomically on MinIO; no directory rename is involved.
"""

import json
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from streaming.silver_transformer import _filesystem, _ready
from observability import telemetry as m9

CONTRACT_VERSION = "1.0"
TABLES = ("events", "revenue_by_minute", "product_rankings")


def gold_root(bucket: str, topic: str) -> str:
    return f"s3a://{bucket}/{topic}/"


def _manifest_files(spark, root):
    fs, path = _filesystem(spark, root.rstrip("/") + "/_commits/")
    if not fs.exists(path):
        return []
    return sorted(s.getPath().toString() for s in fs.listStatus(path)
                  if s.isFile() and s.getPath().getName().endswith(".json"))


def _read_json(spark, path):
    fs, location = _filesystem(spark, path)
    stream = fs.open(location)
    try:
        text = spark.sparkContext._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
        return json.loads(text)
    finally:
        stream.close()


def _validate_manifest(spark: SparkSession, root: str, path: str) -> dict:
    manifest = _read_json(spark, path)
    if (manifest["contract_version"] != CONTRACT_VERSION
            or path.rsplit("/", 1)[1] != f"{manifest['version']:020d}.json"
            or set(manifest["tables"]) != set(TABLES)):
        raise RuntimeError(f"Invalid Gold manifest: {path}")
    for table in manifest["tables"].values():
        if not table["path"].startswith(root.rstrip("/") + "/_snapshots/"):
            raise RuntimeError(f"Gold manifest references a foreign root: {path}")
        if not _ready(spark, table["path"]):
            raise RuntimeError(f"Gold manifest references missing output: {path}")
    return manifest


def manifest_at_version(spark: SparkSession, root: str, version: int) -> dict:
    """Read one explicitly pinned committed manifest for controlled migration tests.

    Normal consumers must use current_manifest(); this helper never falls back and
    validates the exact commit object and all referenced snapshot directories.
    """
    path = root.rstrip("/") + f"/_commits/{int(version):020d}.json"
    return _validate_manifest(spark, root, path)


def current_manifest(spark: SparkSession, root: str) -> dict | None:
    paths = _manifest_files(spark, root)
    if not paths:
        return None
    path = paths[-1]
    return _validate_manifest(spark, root, path)


def read_gold(spark: SparkSession, root: str, metric: str) -> DataFrame:
    """Read a single complete current table. No committed state => typed empty."""
    from streaming.gold_aggregator import OUTPUT_SCHEMAS

    if metric not in OUTPUT_SCHEMAS:
        raise ValueError(f"Unknown Gold metric: {metric}")
    manifest = current_manifest(spark, root)
    if manifest is None:
        return spark.createDataFrame([], OUTPUT_SCHEMAS[metric])
    return spark.read.schema(OUTPUT_SCHEMAS[metric]).parquet(manifest["tables"][metric]["path"])


def read_manifest_table(spark: SparkSession, manifest: dict, metric: str) -> DataFrame:
    """Read one table from an already validated, explicitly pinned manifest."""
    from streaming.gold_aggregator import OUTPUT_SCHEMAS

    if metric not in OUTPUT_SCHEMAS or metric not in manifest.get("tables", {}):
        raise ValueError(f"Unknown or unpinned Gold metric: {metric}")
    return spark.read.schema(OUTPUT_SCHEMAS[metric]).parquet(manifest["tables"][metric]["path"])


def _publish_manifest(spark, root, manifest):
    path = root.rstrip("/") + f"/_commits/{manifest['version']:020d}.json"
    fs, location = _filesystem(spark, path)
    fs.mkdirs(location.getParent())
    if fs.exists(location):
        raise RuntimeError("Concurrent Gold writer detected; require one writer per root")
    stream = fs.create(location, False)
    try:
        stream.write(bytearray(json.dumps(manifest, sort_keys=True).encode("utf-8")))
    finally:
        stream.close()


@m9.observed("gold_commit")
def commit_snapshot(spark, root, tables, *, parent, batch_id, checkpoint, input_count, new_count):
    """Orphan attempts are harmless until the final one-object manifest commit."""
    attempt = root.rstrip("/") + "/_snapshots/" + uuid.uuid4().hex + "/"
    entries = {}
    for name in TABLES:
        df = tables[name]
        path = attempt + name
        df.write.mode("errorifexists").option("compression", "zstd").parquet(path)
        assert _ready(spark, path)
        entries[name] = {"path": path, "rows": spark.read.parquet(path).count(),
                         "schema": df.schema.simpleString()}
    expected_parent = parent["version"] if parent else 0
    observed = current_manifest(spark, root)
    if (observed["version"] if observed else 0) != expected_parent:
        raise RuntimeError("Gold parent changed during write; single-writer contract violated")
    manifest = {
        "contract_version": CONTRACT_VERSION, "version": expected_parent + 1,
        "parent_version": expected_parent, "batch_id": batch_id, "checkpoint": checkpoint,
        "input_records": input_count, "new_events": new_count, "tables": entries,
        "committed_at": datetime.now(timezone.utc).isoformat(),
    }
    _publish_manifest(spark, root, manifest)
    return manifest
