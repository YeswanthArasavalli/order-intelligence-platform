"""Fresh M1 regression using actual generator, publisher, registry and verifier."""

import argparse
from io import BytesIO
import json
import os
import sys

from confluent_kafka.admin import AdminClient, NewTopic
from fastavro import schemaless_reader
from urllib.request import urlopen

from producer import kafka_publisher as publisher
from producer.order_generator import OrderState, make_created, make_paid, make_shipped, make_delivered
from scripts.verify_milestone1_bronze import main as verify_bronze
from scripts.verify_milestone2 import consume


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    args = parser.parse_args()
    topic = args.topic
    assert topic.startswith("order-events-m1-")
    os.environ.update(KAFKA_TOPIC_ORDERS=topic, SPARK_CHECKPOINT_DIR=f"s3a://checkpoints/{topic}")
    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    admin.create_topics([NewTopic(topic, 1, 1)])[topic].result()
    p = publisher.KafkaPublisher(os.environ["KAFKA_BOOTSTRAP_SERVERS"], os.environ["SCHEMA_REGISTRY_URL"], topic)
    state = OrderState("ord-m3-regression", "usr-m3", {"id": "prod-1001", "name": "M3 regression"},
                       "north-india", 100.0, "UPI")
    for ctor in (make_created, make_paid, make_shipped, make_delivered):
        p.publish(ctor(state).to_dict())
    p.flush()
    assert publisher._delivered == 4 and publisher._failed == 0
    with urlopen(os.environ["SCHEMA_REGISTRY_URL"] + f"/subjects/{topic}-value/versions/latest") as response:
        schema = json.load(response)
    assert json.loads(schema["schema"]) == json.loads(publisher.load_schema())
    wire = consume(topic)
    assert len(wire) == 4
    for msg in wire:
        assert msg.value()[0] == 0 and int.from_bytes(msg.value()[1:5], "big") == schema["id"]
        assert schemaless_reader(BytesIO(msg.value()[5:]), json.loads(schema["schema"]))["order_id"] == state.order_id
    print("M1_PRODUCER_VERIFIED " + json.dumps({"topic": topic, "delivered": 4, "failed": 0,
        "schema_id": schema["id"], "subject_version": schema["version"], "header_hex": wire[0].value()[:5].hex(),
        "offsets": [m.offset() for m in wire], "registered_schema_matches": True}))
    sys.argv = [sys.argv[0], "--expected-count", "4"]
    verify_bronze()


if __name__ == "__main__":
    main()
