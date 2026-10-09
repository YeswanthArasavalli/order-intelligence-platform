"""Thin invocations of frozen APIs; no business implementation or scheduling."""
import argparse
import json
import os
from pathlib import Path
import signal
import sys

def orderly_exit(_signal, _frame):
    # Unwind the frozen server's existing finally/close path, including as PID1.
    raise SystemExit(0)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('role', choices=['m4', 'drain', 'fixture', 'receiver', 'exporter', 'm8-baseline', 'receipts', 'tuple'])
    parser.add_argument('--topic')
    parser.add_argument('--run')
    args = parser.parse_args()
    if args.role == 'tuple':
        import platform
        import pyspark
        from streaming.spark_session import get_spark_session
        spark = get_spark_session('Packaged-Frozen-Tuple')
        try:
            jvm = spark.sparkContext._jvm
            actual = {'spark': spark.version, 'pyspark': pyspark.__version__, 'driver': platform.python_version(), 'workers': spark.sparkContext.parallelize([0,1],2).map(lambda _: __import__('platform').python_version()).collect(), 'scala': jvm.scala.util.Properties.versionNumberString(), 'hadoop': jvm.org.apache.hadoop.util.VersionInfo.getVersion(), 'aws_sdk_bundle': jvm.com.amazonaws.util.VersionInfoUtils.getVersion(), 'jdbc': jvm.org.postgresql.util.DriverInfo.DRIVER_VERSION, 'java_runtime': jvm.java.lang.System.getProperty('java.runtime.version')}
            assert actual['spark'] == actual['pyspark'] == '3.5.9'
            assert actual['driver'] == '3.10.12' and actual['workers'] == ['3.10.12','3.10.12']
            assert actual['scala'] == '2.12.18' and actual['hadoop'] == '3.3.4' and actual['aws_sdk_bundle'] == '1.12.262'
            assert actual['jdbc'] == '42.7.3' and actual['java_runtime'] == '17.0.19+10'
            print('PACKAGED_TUPLE_VERIFIED ' + json.dumps(actual))
        finally:
            spark.stop()
    elif args.role == 'm4':
        from streaming.spark_session import get_spark_session
        from storage.gold_postgres_serving import serve_current_gold
        spark = get_spark_session('Packaged-M4')
        try:
            topic = args.topic or os.environ['KAFKA_TOPIC_ORDERS']
            print(json.dumps(serve_current_gold(spark, f's3a://gold/{topic}/', topic), default=str))
        finally:
            spark.stop()
    elif args.role == 'drain':
        from streaming.spark_session import get_spark_session
        from streaming.bronze_writer import read_kafka_stream, write_to_bronze
        from streaming.silver_transformer import read_bronze_stream, write_to_silver
        from streaming.gold_aggregator import read_silver_stream, write_verified_silver_to_gold
        from scripts.verify_milestone3 import drain
        spark = get_spark_session('Packaged-Bounded-Pipeline')
        topic = args.topic or os.environ['KAFKA_TOPIC_ORDERS']
        os.environ.update(KAFKA_TOPIC_ORDERS=topic, KAFKA_TOPIC_DLQ=topic + '-dlq', SPARK_CHECKPOINT_DIR=f's3a://checkpoints/{topic}')
        progress = []
        try:
            drain(read_kafka_stream(spark, topic, os.environ['KAFKA_BOOTSTRAP_SERVERS']), write_to_bronze, f's3a://checkpoints/{topic}/bronze/', progress)
            drain(read_bronze_stream(spark, topic), write_to_silver, f's3a://checkpoints/{topic}/silver/', progress)
            drain(read_silver_stream(spark, topic), write_verified_silver_to_gold, f's3a://checkpoints/{topic}/gold/', progress)
            print(json.dumps({'topic': topic, 'progress': progress}, default=str))
        finally:
            spark.stop()
    elif args.role == 'fixture':
        from confluent_kafka.admin import AdminClient, NewTopic
        from producer.kafka_publisher import KafkaPublisher
        from producer.order_generator import OrderState, make_created, make_paid, make_shipped, make_delivered
        topic = args.topic
        admin = AdminClient({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS']})
        for f in admin.create_topics([NewTopic(topic, 1, 1), NewTopic(topic + '-dlq', 1, 1)]).values():
            f.result()
        p = KafkaPublisher(os.environ['KAFKA_BOOTSTRAP_SERVERS'], os.environ['SCHEMA_REGISTRY_URL'], topic)
        state = OrderState('ord-packaged', 'usr-packaged', {'id': 'prod-1001', 'name': 'Packaged'}, 'north-india', 100.0, 'UPI')
        for ctor in (make_created, make_paid, make_shipped, make_delivered):
            p.publish(ctor(state).to_dict())
        p.flush()
        print(json.dumps({'topic': topic, 'published': 4}))
    elif args.role == 'receiver':
        from scripts.milestone8_receiver import Receiver
        server = Receiver('/tmp/omnirush/packaged-receiver.sqlite', 18088)
        signal.signal(signal.SIGTERM, orderly_exit)
        signal.signal(signal.SIGINT, orderly_exit)
        try:
            server.serve_forever()
        finally:
            server.server_close()
    elif args.role == 'exporter':
        from observability.metrics_exporter import main as serve
        signal.signal(signal.SIGTERM, orderly_exit)
        signal.signal(signal.SIGINT, orderly_exit)
        serve()
    elif args.role == 'm8-baseline':
        from scripts.verify_milestone8 import source_integrity, frozen_files
        from storage.postgres_redis_serving import get_postgres_engine
        engine = get_postgres_engine()
        try:
            result = {'m7_source': source_integrity(engine), 'frozen_files': frozen_files()}
            folder = Path('/opt/oip/milestone8_evidence') / args.run
            folder.mkdir(parents=True, exist_ok=True)
            p = folder / 'inspect-packaging.json'
            assert not p.exists()
            p.write_text(json.dumps({'result': result}, default=str, sort_keys=True))
            print(json.dumps({'fresh_m8_baseline': str(p)}))
        finally:
            engine.dispose()
    else:
        from scripts.milestone8_receiver import Receiver
        from scripts.verify_milestone8 import independent_notifications
        from storage.postgres_redis_serving import get_postgres_engine
        server = Receiver('/tmp/omnirush/packaged-receiver.sqlite', 0)
        engine = get_postgres_engine()
        try:
            records = server.records()
            result = independent_notifications(engine, 'm8_packaged', records)
            assert result['delivery_counts'] == {'DELIVERED': 2}
            assert len(records['receipts']) == 2
            print(json.dumps({'packaged_receipts': records, 'sql': result}, default=str))
        finally:
            server.server_close()
            engine.dispose()

if __name__ == '__main__':
    main()
