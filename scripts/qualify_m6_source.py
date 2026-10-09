"""Explicit M6 qualification: publish retained committed Gold using M4's normal API.

This verification/setup script is NOT imported by the dashboard. It uses M4's
current-only entrypoint into gold_serving, never copies a historical SQL schema.
"""

import json

from storage.gold_postgres_serving import get_engine, serve_current_gold
from storage.gold_store import current_manifest
from streaming.spark_session import get_spark_session
from scripts.verify_milestone4 import report_state


def main():
    spark = get_spark_session("M6-production-source-qualification")
    engine = get_engine()
    topic = "order-events-m3-20261003b"
    root = f"s3a://gold/{topic}/"
    try:
        result = serve_current_gold(spark, root, topic, engine=engine, schema="gold_serving")
        equality = report_state(spark, root, "gold_serving", current_manifest(spark, root))
        print("MILESTONE6_VERIFIED " + json.dumps({"qualification": result, "equality": equality}, default=str, sort_keys=True))
    finally:
        engine.dispose()
        spark.stop()


if __name__ == "__main__":
    main()
