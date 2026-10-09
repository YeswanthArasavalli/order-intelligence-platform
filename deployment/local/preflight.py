"""Fail before invocation when a declared runtime dependency is absent/wrong."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

def check(spark=False, worker=None, jar_root='/opt/pack/jars'):
    spark = spark or os.getenv('OIP_LOCAL_ROLE') == 'spark'
    expected = '3.10.12' if spark else ('3.11.17' if os.getenv('OIP_LOCAL_ROLE') == 'exporter' else '3.11.9')
    actual = sys.version.split()[0]
    if actual != expected:
        raise RuntimeError(f'Python mismatch: expected {expected}, got {actual}')
    if spark:
        worker = worker or sys.executable
        child = subprocess.check_output([worker, '-c', 'import sys; print(sys.version.split()[0])'], text=True).strip()
        if child != actual:
            raise RuntimeError(f'Spark driver/worker Python mismatch: {actual} != {child}')
        inventory = json.loads((Path(jar_root) / 'inventory.json').read_text())
        for row in inventory:
            p = Path(jar_root) / row['filename']
            if not p.exists():
                raise RuntimeError(f'Missing Spark connector artifact: {p.name}')
            if hashlib.sha256(p.read_bytes()).hexdigest() != row['sha256']:
                raise RuntimeError(f'Spark connector checksum mismatch: {p.name}')
        assert len(inventory) == 18
    return {'python': actual, 'worker': worker, 'spark_role': spark, 'verified': True}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--spark', action='store_true')
    parser.add_argument('--worker')
    parser.add_argument('--jar-root', default='/opt/pack/jars')
    args = parser.parse_args()
    print(json.dumps(check(args.spark, args.worker, args.jar_root)))
